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
package org.apache.rocketmq.broker.transaction.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;
    private static final int MAX_RETRY_TIMES_FOR_ESCAPE = 10;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    private static final int OP_MSG_PULL_NUMS = 32;

    private static final int SLEEP_WHILE_NO_OP = 1000;

    private final ConcurrentHashMap<Integer, MessageQueueOpContext> deleteContext = new ConcurrentHashMap<>();

    private ServiceThread transactionalOpBatchService;

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
        transactionalOpBatchService = new TransactionalOpBatchService(transactionalMessageBridge.getBrokerController(), this);
        transactionalOpBatchService.start();
    }


    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            // 半事务消息Topic
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            // 理论上只会有1个队列
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                // 获取RMQ_SYS_TRANS_OP_HALF_TOPIC队列
                MessageQueue opQueue = getOpQueue(messageQueue);
                // 获取Half Topic的消费位点
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                // 获取Op Half Topic的消费位点
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                List<Long> doneOpOffset = new ArrayList<>();
                HashMap<Long/*Half Topic QueueOffset*/, /*OP Half Topic QueueOffset */Long> removeMap = new HashMap<>();
                HashMap<Long/*OP QueueOffset*/,  /*half queueOffset*/HashSet<Long>> opMsgMap = new HashMap<Long, HashSet<Long>>();
                // 1. 将事务已提交或回滚，还未处理(事务有更新状态，来源于客户端主动提交事务或事务回查)的
                // 保存到 removeMap<half QueueOffset, OP QueueOffset>中
                // 保存到 opMsgMap<OP QueueOffset , half QueueOffset>中
                // 2. 将不需要处理的OP消息保存到doneOpOffset，里面存储数据为OP QueueOffset
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, opMsgMap, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;
                long i = halfOffset;
                long nextOpOffset = pullResult.getNextBeginOffset();
                int putInQueueCount = 0;
                int escapeFailCnt = 0;

                while (true) {
                    // 如果处理回查的时间超过60S直接退出
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time rxeach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    // 能在OP消息中查到该Offset，表示事务已经被更新状态，则不需要回查
                    if (removeMap.containsKey(i)) {
                        log.debug("Half offset {} has been committed/rolled back", i);
                        Long removedOpOffset = removeMap.remove(i);
                        opMsgMap.get(removedOpOffset).remove(i);
                        if (opMsgMap.get(removedOpOffset).size() == 0) {
                            opMsgMap.remove(removedOpOffset);
                            doneOpOffset.add(removedOpOffset);
                        }
                    } else {
                        // 根据Offset查询Half消息
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        if (this.transactionalMessageBridge.getBrokerController().getBrokerConfig().isEnableSlaveActingMaster()
                            && this.transactionalMessageBridge.getBrokerController().getMinBrokerIdInGroup()
                            == this.transactionalMessageBridge.getBrokerController().getBrokerIdentity().getBrokerId()
                            && BrokerRole.SLAVE.equals(this.transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getBrokerRole())
                        ) {
                            final MessageExtBrokerInner msgInner = this.transactionalMessageBridge.renewHalfMessageInner(msgExt);
                            final boolean isSuccess = this.transactionalMessageBridge.escapeMessage(msgInner);

                            if (isSuccess) {
                                escapeFailCnt = 0;
                                newOffset = i + 1;
                                i++;
                            } else {
                                log.warn("Escaping transactional message failed {} times! msgId(offsetId)={}, UNIQ_KEY(transactionId)={}",
                                    escapeFailCnt + 1,
                                    msgExt.getMsgId(),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                                if (escapeFailCnt < MAX_RETRY_TIMES_FOR_ESCAPE) {
                                    escapeFailCnt++;
                                    Thread.sleep(100L * (2 ^ escapeFailCnt));
                                } else {
                                    escapeFailCnt = 0;
                                    newOffset = i + 1;
                                    i++;
                                }
                            }
                            continue;
                        }

                        // 消息丢弃条件
                        // 1. 回查次数超过15
                        // 2. 消息已经过期，超过了过期策略配置的时间
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            // 丢弃消息到系统Topic TRANS_CHECK_MAXTIME_TOPIC 中。
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        // 如果在检查期间，有新的Half消息写入，则结束这一轮检测，等待下一周期检测
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        // 消息已保存时间
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        // checkImmunityTime：立即检测事务消息的时间，transactionTimeout：事务消息的超时时间
                        long checkImmunityTime = transactionTimeout;
                        // 如果消息本身指定了回查的时间，则使用消息本身指定的
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            // 如果消息已经保存的时间 < 回查时间
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                // 如果half消息已经被客户端主动的提交或回滚，则将opHalfOffset添加到doneOpOffset
                                // 将Half消息再次发到Half Topic中
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTimeStr)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            // 如果此条Half消息的存储时间小于回查时间(默认6S)，则结束此次检测
                            if (0 <= valueOfCurrentMinusBorn && valueOfCurrentMinusBorn < checkImmunityTime) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        List<MessageExt> opMsg = pullResult == null ? null : pullResult.getMsgFoundList();
                        // 回查条件：
                        // 1. 消息存储的时间已经大于了回查的时间
                        // 2. 如果这批消息不为空，且这批消息中最后一条消息的存储时间 - 当前时间
                        // 3.
                        boolean isNeedCheck = opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime
                            || opMsg != null && opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout
                            || valueOfCurrentMinusBorn <= -1;

                        // 是否需要回查
                        if (isNeedCheck) {
                            // 如果需要回查事务消息，需要将事务消息再次发送到Half Topic中，防止此条消息卡住后面的消息回查
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            putInQueueCount++;
                            log.info("Check transaction. real_topic={},uniqKey={},offset={},commitLogOffset={}",
                                    msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                                    msgExt.getQueueOffset(), msgExt.getCommitLogOffset());
                            // 进行事务回查
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            nextOpOffset = pullResult != null ? pullResult.getNextBeginOffset() : nextOpOffset;
                            pullResult = fillOpRemoveMap(removeMap, opQueue, nextOpOffset,
                                    halfOffset, opMsgMap, doneOpOffset);
                            if (pullResult == null || pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
                                    || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                                    || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {

                                try {
                                    Thread.sleep(SLEEP_WHILE_NO_OP);
                                } catch (Throwable ignored) {
                                }

                            } else {
                                log.info("The miss message offset:{}, pullOffsetOfOp:{}, miniOffset:{} get more opMsg.", i, nextOpOffset, halfOffset);
                            }

                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                // 更新Half Queue消费进度
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    // 更新op Queue消费进度
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
                GetResult getResult = getHalfMsg(messageQueue, newOffset);
                pullResult = pullOpMsg(opQueue, newOpOffset, 1);
                long maxMsgOffset = getResult.getPullResult() == null ? newOffset : getResult.getPullResult().getMaxOffset();
                long maxOpOffset = pullResult == null ? newOpOffset : pullResult.getMaxOffset();
                long msgTime = getResult.getMsg() == null ? System.currentTimeMillis() : getResult.getMsg().getStoreTimestamp();

                log.info("After check, {} opOffset={} opOffsetDiff={} msgOffset={} msgOffsetDiff={} msgTime={} msgTimeDelayInMs={} putInQueueCount={}",
                        messageQueue, newOpOffset, maxOpOffset - newOpOffset, newOffset, maxMsgOffset - newOffset, new Date(msgTime),
                        System.currentTimeMillis() - msgTime, putInQueueCount);
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset The current minimum offset of half message queue.
     * @param opMsgMap Half message offset in op message
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap, MessageQueue opQueue,
                                       long pullOffsetOfOp, long miniOffset, Map<Long, HashSet<Long>> opMsgMap, List<Long> doneOpOffset) {
        // 拉取OP消息
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, OP_MSG_PULL_NUMS);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        for (MessageExt opMessageExt : opMsg) {
            if (opMessageExt.getBody() == null) {
                log.error("op message body is null. queueId={}, offset={}", opMessageExt.getQueueId(),
                        opMessageExt.getQueueOffset());
                doneOpOffset.add(opMessageExt.getQueueOffset());
                continue;
            }
            HashSet<Long> set = new HashSet<Long>();
            String queueOffsetBody = new String(opMessageExt.getBody(), TransactionalMessageUtil.CHARSET);

            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffsetBody);
            if (TransactionalMessageUtil.REMOVE_TAG.equals(opMessageExt.getTags())) {
                String[] offsetArray = queueOffsetBody.split(TransactionalMessageUtil.OFFSET_SEPARATOR);
                for (String offset : offsetArray) {
                    Long offsetValue = getLong(offset);
                    if (offsetValue < miniOffset) {
                        continue;
                    }

                    removeMap.put(offsetValue, opMessageExt.getQueueOffset());
                    set.add(offsetValue);
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }

            if (set.size() > 0) {
                opMsgMap.put(opMessageExt.getQueueOffset(), set);
            } else {
                doneOpOffset.add(opMessageExt.getQueueOffset());
            }
        }

        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        log.debug("opMsg map: {}", opMsgMap);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt, String checkImmunityTimeStr) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                // 这个消息已经被回查处理了
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    log.info("removeMap contain prepareQueueOffset. real_topic={},uniqKey={},immunityTime={},offset={}",
                            msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC),
                            msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                            checkImmunityTimeStr,
                            msgExt.getQueueOffset());
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            // RMQ_SYS_TRANS_OP_HALF_TOPIC
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        if (result != null) {
            getResult.setPullResult(result);
            List<MessageExt> messageExts = result.getMsgFoundList();
            if (messageExts == null || messageExts.size() == 0) {
                return getResult;
            }
            getResult.setMsg(messageExts.get(0));
        }
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt messageExt) {
        Integer queueId = messageExt.getQueueId();
        MessageQueueOpContext mqContext = deleteContext.get(queueId);
        if (mqContext == null) {
            mqContext = new MessageQueueOpContext(System.currentTimeMillis(), 20000);
            MessageQueueOpContext old = deleteContext.putIfAbsent(queueId, mqContext);
            if (old != null) {
                mqContext = old;
            }
        }

        String data = messageExt.getQueueOffset() + TransactionalMessageUtil.OFFSET_SEPARATOR;
        try {
            boolean res = mqContext.getContextQueue().offer(data, 100, TimeUnit.MILLISECONDS);
            if (res) {
                int totalSize = mqContext.getTotalSize().addAndGet(data.length());
                if (totalSize > transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize()) {
                    this.transactionalOpBatchService.wakeup();
                }
                return true;
            } else {
                this.transactionalOpBatchService.wakeup();
            }
        } catch (InterruptedException ignore) {
        }

        Message msg = getOpMessage(queueId, data);
        // 将OP消息写入到RMQ_SYS_TRANS_OP_HALF_TOPIC中
        if (this.transactionalMessageBridge.writeOp(queueId, msg)) {
            log.warn("Force add remove op data. queueId={}", queueId);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", messageExt.getMsgId(), messageExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

    public Message getOpMessage(int queueId, String moreData) {
        String opTopic = TransactionalMessageUtil.buildOpTopic();
        MessageQueueOpContext mqContext = deleteContext.get(queueId);

        int moreDataLength = moreData != null ? moreData.length() : 0;
        int length = moreDataLength;
        int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
        // 如果此条消息追加的数据 < op消息最大值
        if (length < maxSize) {
            // 当前
            int sz = mqContext.getTotalSize().get();
            if (sz > maxSize || length + sz > maxSize) {
                length = maxSize + 100;
            } else {
                length += sz;
            }
        }

        // 1，2，3，4，
        StringBuilder sb = new StringBuilder(length);

        if (moreData != null) {
            sb.append(moreData);
        }

        while (!mqContext.getContextQueue().isEmpty()) {
            if (sb.length() >= maxSize) {
                break;
            }
            String data = mqContext.getContextQueue().poll();
            if (data != null) {
                sb.append(data);
            }
        }

        if (sb.length() == 0) {
            return null;
        }

        int l = sb.length() - moreDataLength;
        mqContext.getTotalSize().addAndGet(-l);
        mqContext.setLastWriteTimestamp(System.currentTimeMillis());
        // RMQ_SYS_TRANS_OP_HALF_TOPIC
        return new Message(opTopic, TransactionalMessageUtil.REMOVE_TAG,
                sb.toString().getBytes(TransactionalMessageUtil.CHARSET));
    }
    public long batchSendOpMessage() {
        long startTime = System.currentTimeMillis();
        try {
            long firstTimestamp = startTime;
            Map<Integer, Message> sendMap = null;
            long interval = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpBatchInterval();
            int maxSize = transactionalMessageBridge.getBrokerController().getBrokerConfig().getTransactionOpMsgMaxSize();
            boolean overSize = false;
            for (Map.Entry<Integer, MessageQueueOpContext> entry : deleteContext.entrySet()) {
                MessageQueueOpContext mqContext = entry.getValue();
                //no msg in contextQueue
                if (mqContext.getTotalSize().get() <= 0 || mqContext.getContextQueue().size() == 0 ||
                        // wait for the interval
                        mqContext.getTotalSize().get() < maxSize &&
                                startTime - mqContext.getLastWriteTimestamp() < interval) {
                    continue;
                }

                if (sendMap == null) {
                    sendMap = new HashMap<>();
                }

                Message opMsg = getOpMessage(entry.getKey(), null);
                if (opMsg == null) {
                    continue;
                }
                sendMap.put(entry.getKey(), opMsg);
                firstTimestamp = Math.min(firstTimestamp, mqContext.getLastWriteTimestamp());
                if (mqContext.getTotalSize().get() >= maxSize) {
                    overSize = true;
                }
            }

            if (sendMap != null) {
                for (Map.Entry<Integer, Message> entry : sendMap.entrySet()) {
                    if (!this.transactionalMessageBridge.writeOp(entry.getKey(), entry.getValue())) {
                        log.error("Transaction batch op message write failed. body is {}, queueId is {}",
                                new String(entry.getValue().getBody(), TransactionalMessageUtil.CHARSET), entry.getKey());
                    }
                }
            }

            log.debug("Send op message queueIds={}", sendMap == null ? null : sendMap.keySet());

            //wait for next batch remove
            long wakeupTimestamp = firstTimestamp + interval;
            if (!overSize && wakeupTimestamp > startTime) {
                return wakeupTimestamp;
            }
        } catch (Throwable t) {
            log.error("batchSendOp error.", t);
        }

        return 0L;
    }

    public Map<Integer, MessageQueueOpContext> getDeleteContext() {
        return this.deleteContext;
    }
}
