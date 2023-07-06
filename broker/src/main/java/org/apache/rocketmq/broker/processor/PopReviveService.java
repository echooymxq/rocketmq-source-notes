/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

public class PopReviveService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);

    private int queueId;
    private BrokerController brokerController;
    private String reviveTopic;
    private long currentReviveMessageTimestamp = -1;
    private volatile boolean shouldRunPopRevive = false;

    private final NavigableMap<PopCheckPoint/* oldCK */, Pair<Long/* timestamp */, Boolean/* result */>> inflightReviveRequestMap = Collections.synchronizedNavigableMap(new TreeMap<>());
    private long reviveOffset;

    public PopReviveService(BrokerController brokerController, String reviveTopic, int queueId) {
        this.queueId = queueId;
        this.brokerController = brokerController;
        this.reviveTopic = reviveTopic;
        this.reviveOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + "PopReviveService_" + this.queueId;
        }
        return "PopReviveService_" + this.queueId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setShouldRunPopRevive(final boolean shouldRunPopRevive) {
        this.shouldRunPopRevive = shouldRunPopRevive;
    }

    public boolean isShouldRunPopRevive() {
        return shouldRunPopRevive;
    }

    // 根据 CheckPoint 找到没有被 ACK 的消息，发到重试队列
    private boolean reviveRetry(PopCheckPoint popCheckPoint, MessageExt messageExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        if (!popCheckPoint.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            //%RETRY%ClientId_Topic
            msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getTopic(), popCheckPoint.getCId()));
        } else {
            msgInner.setTopic(popCheckPoint.getTopic());
        }
        msgInner.setBody(messageExt.getBody());
        msgInner.setQueueId(0);
        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<>());
        }
        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        // 重试次数 += 1
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        msgInner.getProperties().putAll(messageExt.getProperties());
        // 如果这个消息是第一次重试，则设置重试消息的属性1ST_POP_TIME的Pop时间
        if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPopTime()));
        }
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        // 如果重试Topic不存在，则创建该Topic
        addRetryTopicIfNoExit(msgInner.getTopic(), popCheckPoint.getCId());
        // 将没有被ACK的消息重新投递到重试Topic中去.
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        PopMetricsManager.incPopReviveRetryMessageCount(popCheckPoint, putMessageResult.getPutMessageStatus());
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={},retry msg , ck={}, msg queueId {}, offset {}, reviveDelay={}, result is {} ",
                queueId, popCheckPoint, messageExt.getQueueId(), messageExt.getQueueOffset(),
                (System.currentTimeMillis() - popCheckPoint.getReviveTime()) / 1000, putMessageResult);
        }
        if (putMessageResult.getAppendMessageResult() == null ||
            putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            POP_LOGGER.error("reviveQueueId={}, revive error, msg is: {}", queueId, msgInner);
            return false;
        }

        // ... 更新统计数据
        this.brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(popCheckPoint);
        this.brokerController.getBrokerStatsManager().incBrokerPutNums(popCheckPoint.getTopic(), 1);
        this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
        this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());

        if (brokerController.getPopMessageProcessor() != null) {
            brokerController.getPopMessageProcessor().notifyMessageArriving(
                KeyBuilder.parseNormalTopic(popCheckPoint.getTopic(), popCheckPoint.getCId()),
                popCheckPoint.getCId(),
                -1
            );
            brokerController.getNotificationProcessor().notifyMessageArriving(
                KeyBuilder.parseNormalTopic(popCheckPoint.getTopic(), popCheckPoint.getCId()), -1);
        }
        return true;
    }

    private void initPopRetryOffset(String topic, String consumerGroup) {
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(consumerGroup, topic, 0);
        if (offset < 0) {
            this.brokerController.getConsumerOffsetManager().commitOffset("initPopRetryOffset", consumerGroup, topic,
                0, 0);
        }
    }

    private void addRetryTopicIfNoExit(String topic, String consumerGroup) {
        if (brokerController != null) {
            TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig != null) {
                return;
            }
            topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(PopAckConstants.retryQueueNum); //1
            topicConfig.setWriteQueueNums(PopAckConstants.retryQueueNum); //1
            topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
            topicConfig.setPerm(6);
            topicConfig.setTopicSysFlag(0);
            brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

            initPopRetryOffset(topic, consumerGroup);
        }
    }

    protected List<MessageExt> getReviveMessage(long offset, int queueId) {
        PullResult pullResult = getMessage(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32, true);
        if (pullResult == null) {
            return null;
        }
        // 表示此次拉取到消息，但是请求的offset过大
        if (reachTail(pullResult, offset)) {
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("reviveQueueId={}, reach tail,offset {}", queueId, offset);
            }
            // PullStatus 为 OFFSET_ILLEGAL 没有拉到消息
        } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            POP_LOGGER.error("reviveQueueId={}, OFFSET_ILLEGAL {}, result is {}", queueId, offset, pullResult);
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip offset correct topic={}, reviveQueueId={}", reviveTopic, queueId);
                return null;
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, pullResult.getNextBeginOffset() - 1);
        }
        return pullResult.getMsgFoundList();
    }

    private boolean reachTail(PullResult pullResult, long offset) {
        return pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
            || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL && offset == pullResult.getMaxOffset();
    }

    private CompletableFuture<Pair<GetMessageStatus, MessageExt>> getBizMessage(String topic, long offset, int queueId,
        String brokerName) {
        return this.brokerController.getEscapeBridge().getMessageAsync(topic, offset, queueId, brokerName, false);
    }

    public PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        boolean deCompressBody) {
        GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    pullStatus = PullStatus.FOUND;
                    foundList = decodeMsgList(getMessageResult, deCompressBody);
                    brokerController.getBrokerStatsManager().incGroupGetNums(group, topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().incGroupGetSize(group, topic, getMessageResult.getBufferTotalSize());
                    brokerController.getBrokerStatsManager().incBrokerGetNums(topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());

                    Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                        .put(LABEL_TOPIC, topic)
                        .put(LABEL_CONSUMER_GROUP, group)
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic) || MixAll.isSysConsumerGroup(group))
                        .build();
                    BrokerMetricsManager.messagesOutTotal.add(getMessageResult.getMessageCount(), attributes);
                    BrokerMetricsManager.throughputOutTotal.add(getMessageResult.getBufferTotalSize(), attributes);

                    break;
                case NO_MATCHED_MESSAGE:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    POP_LOGGER.debug("no matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    POP_LOGGER.debug("no new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_TOO_SMALL:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    POP_LOGGER.warn("offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case OFFSET_OVERFLOW_ONE:
                    // no need to print WARN, because we use "offset + 1" to get the next message
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    break;
                default:
                    assert false;
                    break;
            }

            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            long maxQueueOffset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
            if (maxQueueOffset > offset) {
                POP_LOGGER.error("get message from store return null. topic={}, groupId={}, requestOffset={}, maxQueueOffset={}",
                    topic, group, offset, maxQueueOffset);
            }
            return null;
        }
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        POP_LOGGER.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                    if (msgExt == null) {
                        POP_LOGGER.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /*
     * 消费Revive Topic中的消息，匹配ACK消息和CheckPoint
     * CheckPoint的消息放到Map中，ACK的消息根据Map key匹配CheckPoint的消息，更新CheckPoint消息的码表以完成ACK
     * 只对CheckPoint进行标记
     * 消费时间差2s内的CheckPoint、ACK 消息，或 4s 没有消费到新消息
     *
     * @param consumeReviveObj CheckPoint与ACK匹配对象，用于 Revive 需要重试 Pop 消费的消息
     */
    protected void consumeReviveMessage(ConsumeReviveObj consumeReviveObj) {
        // MergeKey = topic+cid+queueId+startOffset+popTime
        HashMap<String/*MergeKey*/, PopCheckPoint> map = consumeReviveObj.map;
        HashMap<String, PopCheckPoint> mockPointMap = new HashMap<>();
        long startScanTime = System.currentTimeMillis();
        long endTime = 0;
        // 查询当前的消费进度offset
        long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
        long oldOffset = Math.max(reviveOffset, consumeOffset);
        consumeReviveObj.oldOffset = oldOffset;
        POP_LOGGER.info("reviveQueueId={}, old offset is {} ", queueId, oldOffset);
        long offset = oldOffset + 1;
        // 没有查询到消息的次数
        int noMsgCount = 0;
        long firstRt = 0;
        // offset self amend
        while (true) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip scan , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            // 查询一批 Revive Topic 中的消息（32条）
            List<MessageExt> messageExts = getReviveMessage(offset, queueId);
            if (messageExts == null || messageExts.isEmpty()) {
                long old = endTime;
                // 延时消息的延迟
                long timerDelay = brokerController.getMessageStore().getTimerMessageStore().getDequeueBehind();
                long commitLogDelay = brokerController.getMessageStore().getTimerMessageStore().getEnqueueBehind();
                // move endTime
                // endTime不为0，表示前面已经拉取到过消息了
                // 当前时间-最后读取到消息到时间 > 3秒且延时消息里面没有数据
                if (endTime != 0 && System.currentTimeMillis() - endTime > 3 * PopAckConstants.SECOND && timerDelay <= 0 && commitLogDelay <= 0) {
                    endTime = System.currentTimeMillis();
                }
                POP_LOGGER.info("reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new endTime {}, timerDelay={}, commitLogDelay={} ",
                    queueId, offset, old, endTime, timerDelay, commitLogDelay);
                // 最后一个 CK 的唤醒时间与第一个 CK 的唤醒时间差大于 2s，中断消费。
                // Fixme: 为什么是2S?
                // TODO 这里是不是应该加一个firstRt != 0 的判断？
                if (endTime - firstRt > PopAckConstants.ackTimeInterval + PopAckConstants.SECOND) {
                    break;
                }
                noMsgCount++;
                // Fixme: why sleep is useful here?
                try {
                    // 避免拉不到消息一直循环，消耗CPU
                    Thread.sleep(100);
                } catch (Throwable ignore) {
                }
                // 1s noMsgCount=10
                // 1s 1000
                // 4s 4000
                // 这里每次拉到空消息就sleep100ms * 100，连续 4s 没有消费到新的消息，中断消费。
                if (noMsgCount * 100L > 4 * PopAckConstants.SECOND) {
                    break;
                } else {
                    continue;
                }
            } else {
                noMsgCount = 0;
            }
            // 10S 结束循环。
            if (System.currentTimeMillis() - startScanTime > brokerController.getBrokerConfig().getReviveScanTime()) {
                POP_LOGGER.info("reviveQueueId={}, scan timeout  ", queueId);
                break;
            }
            // 遍历查询到的消息
            // 这里的消息包括了CK和ACK的，那么这些ACK消息中有可能包含了其它消费者组的
            for (MessageExt messageExt : messageExts) {
                // 如果是 CheckPoint
                if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), DataConverter.charset);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
                    if (point.getTopic() == null || point.getCId() == null) {
                        continue;
                    }
                    // 将此CK放入到Map中，等待 ACK 消息匹配
                    map.put(point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime(), point);
                    PopMetricsManager.incPopReviveCkGetCount(point, queueId);
                    // 设置 reviveOffset 为 revive 队列中消息的逻辑 offset
                    point.setReviveOffset(messageExt.getQueueOffset());
                    if (firstRt == 0) {
                        firstRt = point.getReviveTime();
                    }
                } else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {
                    // 如果是ACK请求
                    String raw = new String(messageExt.getBody(), DataConverter.charset);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={},find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
                    PopMetricsManager.incPopReviveAckGetCount(ackMsg, queueId);
                    // mergeKey = Topic@ConsumerGroup@QueueId@StartOffset@PopoTime
                    // 找到对应的CheckPoint进行匹配
                    String mergeKey = ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime();
                    PopCheckPoint point = map.get(mergeKey);
                    // 如果找不到，存在的原因：
                    // 1. ACK提前一步比CK消息投递在ReviveTopic中，这一步需要确认一下，因为在代码层面是ACK提前1s投递的
                    if (point == null) {
                        // 是否允许略过长时间ACK超时的这些ACK请求，这里判断有点儿问题// TODO
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            continue;
                        }

                        if (mockCkForAck(messageExt, ackMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        // 如果 HashMap 中有 CheckPoint，计算 ACK 的 bit 码表
                        int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
                        if (indexOfAck > -1) {
                            point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                        } else {
                            POP_LOGGER.error("invalid ack index, {}, {}", ackMsg, point);
                        }
                    }
                } else if (PopAckConstants.BATCH_ACK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), DataConverter.charset);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={}, find batch ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }

                    BatchAckMsg bAckMsg = JSON.parseObject(raw, BatchAckMsg.class);
                    PopMetricsManager.incPopReviveAckGetCount(bAckMsg, queueId);
                    String mergeKey = bAckMsg.getTopic() + bAckMsg.getConsumerGroup() + bAckMsg.getQueueId() + bAckMsg.getStartOffset() + bAckMsg.getPopTime();
                    PopCheckPoint point = map.get(mergeKey);
                    if (point == null) {
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            continue;
                        }
                        if (mockCkForAck(messageExt, bAckMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        List<Long> ackOffsetList = bAckMsg.getAckOffsetList();
                        for (Long ackOffset : ackOffsetList) {
                            int indexOfAck = point.indexOfAck(ackOffset);
                            if (indexOfAck > -1) {
                                point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                            } else {
                                POP_LOGGER.error("invalid batch ack index, {}, {}", bAckMsg, point);
                            }
                        }
                    }
                }
                long deliverTime = messageExt.getDeliverTimeMs();
                // 取所有消息中最大的消息投递时间
                // 如果这一批消息里面包括了多个CK，则endTime为CK中最大的ReviveTime(POP Time + InvisibleTime)
                if (deliverTime > endTime) {
                    endTime = deliverTime;
                }
            }
            offset = offset + messageExts.size();
        }
        consumeReviveObj.map.putAll(mockPointMap);
        consumeReviveObj.endTime = endTime;
    }

    private boolean mockCkForAck(MessageExt messageExt, AckMsg ackMsg, String mergeKey, HashMap<String, PopCheckPoint> mockPointMap) {
        // ACK等待的时间为当前时间-消息投递时间
        // deliverTimeMs是在存入到reviveTopic中时设置的，值为POP时间 + InvisibleTime
        long ackWaitTime = System.currentTimeMillis() - messageExt.getDeliverTimeMs();
        // 默认3分钟
        long reviveAckWaitMs = brokerController.getBrokerConfig().getReviveAckWaitMs();
        if (ackWaitTime > reviveAckWaitMs) {
            // will use the reviveOffset of popCheckPoint to commit offset in mergeAndRevive
            // 如果消息的ACK时间已经超过3分钟, 那么这条ACK请求已经没有意义了，下面创建一个mock CheckPoint，只是为了
            // 在mergeAndRevive的时候更新revive Topic的消费进度，为减少重复消费做一层保障而已。
            // 未被ACK的消息，已经在处理之前的CheckPoint发送到重试Topic了。
            // 每条ACK请求都会创建一个Mock的CheckPoint
            PopCheckPoint mockPoint = createMockCkForAck(ackMsg, messageExt.getQueueOffset());
            POP_LOGGER.warn(
                    "ack wait for {}ms cannot find ck, skip this ack. mergeKey:{}, ack:{}, mockCk:{}",
                    reviveAckWaitMs, mergeKey, ackMsg, mockPoint);
            mockPointMap.put(mergeKey, mockPoint);
            return true;
        }
        return false;
    }

    private PopCheckPoint createMockCkForAck(AckMsg ackMsg, long reviveOffset) {
        PopCheckPoint point = new PopCheckPoint();
        point.setStartOffset(ackMsg.getStartOffset());
        point.setPopTime(ackMsg.getPopTime());
        point.setQueueId(ackMsg.getQueueId());
        point.setCId(ackMsg.getConsumerGroup());
        point.setTopic(ackMsg.getTopic());
        // 这里设置消息数目为0，只是为了在mergeAndRevive的时候进行revive topic的queueOffset的提交
        point.setNum((byte) 0);
        point.setBitMap(0);
        point.setReviveOffset(reviveOffset);
        point.setBrokerName(ackMsg.getBrokerName());
        return point;
    }

    // 匹配消费到的一批 CK 和 ACK 消息，对于没有成功 ACK 的消息，重发到重试 Topic
    protected void mergeAndRevive(ConsumeReviveObj consumeReviveObj) throws Throwable {
        // 获取排序后的 CheckPoint 列表
        ArrayList<PopCheckPoint> sortList = consumeReviveObj.genSortList();
        POP_LOGGER.info("reviveQueueId={},ck listSize={}", queueId, sortList.size());
        if (sortList.size() != 0) {
            POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={} ; last ck, startOffset={}, reviveOffset={}", queueId, sortList.get(0).getStartOffset(),
                sortList.get(0).getReviveOffset(), sortList.get(sortList.size() - 1).getStartOffset(), sortList.get(sortList.size() - 1).getReviveOffset());
        }
        long newOffset = consumeReviveObj.oldOffset;
        // 遍历排序过后的CK
        for (PopCheckPoint popCheckPoint : sortList) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip ck process , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            // 如果没有到 Revive 时间，跳过
            // CK和ACK消息已经是在Revive Topic中了，但是还没有到重新投递到重试Topic的时间。
            // 如果消息的最大投递时间 - CK的投递时间 <= 2S
            // 如果列表中CK的最大时间 - 该CK的时间 <= 2s
            // 如果列表中只有一个CK，那么这个endTime就是该PopCheckPoint的ReviveTime，直接跳过
            // 至少需要2s后的CK
            if (consumeReviveObj.endTime - popCheckPoint.getReviveTime() <= (PopAckConstants.ackTimeInterval + PopAckConstants.SECOND)) {
                break;
            }

            // 从 CK 中解析原 Topic 并检查该 Topic 是否存在，如果不存在则跳过
            // check normal topic, skip ck , if normal topic is not exist
            String normalTopic = KeyBuilder.parseNormalTopic(popCheckPoint.getTopic(), popCheckPoint.getCId());
            if (brokerController.getTopicConfigManager().selectTopicConfig(normalTopic) == null) {
                POP_LOGGER.warn("reviveQueueId={},can not get normal topic {} , then continue ", queueId, popCheckPoint.getTopic());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }
            // 如果客户端已离线
            if (null == brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(popCheckPoint.getCId())) {
                POP_LOGGER.warn("reviveQueueId={},can not get cid {} , then continue ", queueId, popCheckPoint.getCId());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }

            // 如果当前重新投递消息的CheckPoint大于3，则继续等待
            while (inflightReviveRequestMap.size() > 3) {
                waitForRunning(100);
                Pair<Long, Boolean> pair = inflightReviveRequestMap.firstEntry().getValue();
                // 如果第一个CheckPoint没有成功，以及已经超过了30s，则将该CheckPoint直接重新发送到reviveTopic中
                if (!pair.getObject2() && System.currentTimeMillis() - pair.getObject1() > 1000 * 30) {
                    PopCheckPoint oldCK = inflightReviveRequestMap.firstKey();
                    rePutCK(oldCK, pair);
                    inflightReviveRequestMap.remove(oldCK);
                }
            }
            // 重新投递CheckPoint中没有被Ack的消息
            reviveMsgFromCk(popCheckPoint);

            // 处理完一个QueueOffset之后，更新QueueOffset
            newOffset = popCheckPoint.getReviveOffset();
        }
        // 匹配完成后，更新ReviveTopic消费进度
        if (newOffset > consumeReviveObj.oldOffset) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip commit, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                return;
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
        }
        reviveOffset = newOffset;
        consumeReviveObj.newOffset = newOffset;
    }

    // 重发CheckPoint中没有Ack的所有消息
    private void reviveMsgFromCk(PopCheckPoint popCheckPoint) {
        if (!shouldRunPopRevive) {
            POP_LOGGER.info("slave skip retry , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return;
        }
        inflightReviveRequestMap.put(popCheckPoint, new Pair<>(System.currentTimeMillis(), false));
        List<CompletableFuture<Pair<Long, Boolean>>> futureList = new ArrayList<>(popCheckPoint.getNum());
        for (int j = 0; j < popCheckPoint.getNum(); j++) {
            // 如果消息已经被ACK了，跳过
            if (DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
                continue;
            }

            // retry msg
            // 获取实际消息的QueueOffset
            long msgOffset = popCheckPoint.ackOffsetByIndex((byte) j);
            // 查询CheckPoint消息对应的真正消息
            CompletableFuture<Pair<Long, Boolean>> future = getBizMessage(popCheckPoint.getTopic(), msgOffset, popCheckPoint.getQueueId(), popCheckPoint.getBrokerName())
                .thenApply(resultPair -> {
                    GetMessageStatus getMessageStatus = resultPair.getObject1();
                    MessageExt message = resultPair.getObject2();
                    if (message == null) {
                        POP_LOGGER.warn("reviveQueueId={}, can not get biz msg topic is {}, offset is {}, then continue",
                            queueId, popCheckPoint.getTopic(), msgOffset);
                        switch (getMessageStatus) {
                            case MESSAGE_WAS_REMOVING:
                            case OFFSET_TOO_SMALL:
                            case NO_MATCHED_LOGIC_QUEUE:
                            case NO_MESSAGE_IN_QUEUE:
                                return new Pair<>(msgOffset, true);
                            default:
                                return new Pair<>(msgOffset, false);

                        }
                    }
                    //skip ck from last epoch
                    // 如果消息的POP时间 < 消息的存储时间
                    // 什么情况下，POP时间还会比消息的存储时间小?
                    if (popCheckPoint.getPopTime() < message.getStoreTimestamp()) {
                        POP_LOGGER.warn("reviveQueueId={}, skip ck from last epoch {}", queueId, popCheckPoint);
                        return new Pair<>(msgOffset, true);
                    }
                    // 重新投递没有被ACK的消息到重试队列
                    boolean result = reviveRetry(popCheckPoint, message);
                    return new Pair<>(msgOffset, result);
                });
            futureList.add(future);
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
            .whenComplete((v, e) -> {
                for (CompletableFuture<Pair<Long, Boolean>> future : futureList) {
                    Pair<Long, Boolean> pair = future.getNow(new Pair<>(0L, false));
                    if (!pair.getObject2()) { // 如果消息PUT到重试Topic不成功，则重新Put CK to store.
                        rePutCK(popCheckPoint, pair);
                    }
                }

                if (inflightReviveRequestMap.containsKey(popCheckPoint)) {
                    inflightReviveRequestMap.get(popCheckPoint).setObject2(true);
                }
                for (Map.Entry<PopCheckPoint, Pair<Long, Boolean>> entry : inflightReviveRequestMap.entrySet()) {
                    PopCheckPoint oldCK = entry.getKey();
                    Pair<Long, Boolean> pair = entry.getValue();
                    // 如果发送到重试Topic成功，则可以提交revive Topic的offset
                    if (pair.getObject2()) {
                        brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, oldCK.getReviveOffset());
                        inflightReviveRequestMap.remove(oldCK);
                    } else {
                        break;
                    }
                }
            });
    }

    private void rePutCK(PopCheckPoint oldCK, Pair<Long, Boolean> pair) {
        PopCheckPoint newCk = new PopCheckPoint();
        newCk.setBitMap(0);
        newCk.setNum((byte) 1);
        newCk.setPopTime(oldCK.getPopTime());
        newCk.setInvisibleTime(oldCK.getInvisibleTime());
        newCk.setStartOffset(pair.getObject1());
        newCk.setCId(oldCK.getCId());
        newCk.setTopic(oldCK.getTopic());
        newCk.setQueueId(oldCK.getQueueId());
        newCk.addDiff(0);
        MessageExtBrokerInner ckMsg = brokerController.getPopMessageProcessor().buildCkMsg(newCk, queueId);
        brokerController.getMessageStore().putMessage(ckMsg);
    }

    public long getReviveBehindMillis() {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long maxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId);
        if (maxOffset - reviveOffset > 1) {
            return Math.max(0, System.currentTimeMillis() - currentReviveMessageTimestamp);
        }
        return 0;
    }

    public long getReviveBehindMessages() {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long diff = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId) - reviveOffset;
        return Math.max(0, diff);
    }

    @Override
    public void run() {
        int slow = 1;
        while (!this.isStopped()) {
            try {
                if (System.currentTimeMillis() < brokerController.getShouldStartTime()) {
                    POP_LOGGER.info("PopReviveService Ready to run after {}", brokerController.getShouldStartTime());
                    this.waitForRunning(1000);
                    continue;
                }
                // 默认1S
                this.waitForRunning(brokerController.getBrokerConfig().getReviveInterval());
                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("skip start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
                    POP_LOGGER.warn("skip revive topic because timerWheelEnable is false");
                    continue;
                }

                POP_LOGGER.info("start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                ConsumeReviveObj consumeReviveObj = new ConsumeReviveObj();
                consumeReviveMessage(consumeReviveObj);

                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("slave skip scan , revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                mergeAndRevive(consumeReviveObj);

                ArrayList<PopCheckPoint> sortList = consumeReviveObj.sortList;
                long delay = 0;
                if (sortList != null && !sortList.isEmpty()) {
                    // 表示消息从投递到Revive Topic到处理结束的这一段延时为多少
                    delay = (System.currentTimeMillis() - sortList.get(0).getReviveTime()) / 1000;
                    currentReviveMessageTimestamp = sortList.get(0).getReviveTime();
                    slow = 1;
                } else {
                    currentReviveMessageTimestamp = System.currentTimeMillis();
                }

                POP_LOGGER.info("reviveQueueId={},revive finish,old offset is {}, new offset is {}, ckDelay={}  ",
                    queueId, consumeReviveObj.oldOffset, consumeReviveObj.newOffset, delay);

                // 一直没有CK
                if (sortList == null || sortList.isEmpty()) {
                    POP_LOGGER.info("reviveQueueId={}, has no new msg, take a rest {}", queueId, slow);
                    // 等待重新工作
                    this.waitForRunning(slow * brokerController.getBrokerConfig().getReviveInterval());
                    // 最大等待次数为3次
                    if (slow < brokerController.getBrokerConfig().getReviveMaxSlow()) {
                        slow++;
                    }
                }

            } catch (Throwable e) {
                POP_LOGGER.error("reviveQueueId={}, revive error", queueId, e);
            }
        }
    }

    static class ConsumeReviveObj {
        HashMap<String, PopCheckPoint> map = new HashMap<>();
        ArrayList<PopCheckPoint> sortList;
        long oldOffset;
        long endTime;
        long newOffset;

        ArrayList<PopCheckPoint> genSortList() {
            if (sortList != null) {
                return sortList;
            }
            sortList = new ArrayList<>(map.values());
            // PopCheckPoint的reviveOffset是CK在ReviveTopic中的QueueOffset
            // 根据QueueOffset对其进行排序
            sortList.sort((o1, o2) -> (int) (o1.getReviveOffset() - o2.getReviveOffset()));
            return sortList;
        }
    }
}
