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
import com.sun.org.apache.bcel.internal.generic.POP;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class PopBufferMergeService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    ConcurrentHashMap<String/*mergeKey*/, PopCheckPointWrapper> buffer = new ConcurrentHashMap<>(1024 * 16);
    ConcurrentHashMap<String/*topic@cid@queueId*/, QueueWithTime<PopCheckPointWrapper>> commitOffsets = new ConcurrentHashMap<>();
    private volatile boolean serving = true;
    private AtomicInteger counter = new AtomicInteger(0); // CheckPoint的数量
    private int scanTimes = 0;
    private final BrokerController brokerController;
    private final PopMessageProcessor popMessageProcessor;
    private final PopMessageProcessor.QueueLockManager queueLockManager;
    private final long interval = 5;
    private final long minute5 = 5 * 60 * 1000;
    private final int countOfMinute1 = (int) (60 * 1000 / interval);
    private final int countOfSecond1 = (int) (1000 / interval);
    private final int countOfSecond30 = (int) (30 * 1000 / interval);

    private volatile boolean master = false;

    public PopBufferMergeService(BrokerController brokerController, PopMessageProcessor popMessageProcessor) {
        this.brokerController = brokerController;
        this.popMessageProcessor = popMessageProcessor;
        this.queueLockManager = popMessageProcessor.getQueueLockManager();
    }

    private boolean isShouldRunning() {
        if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()) {
            return true;
        }
        this.master = brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        return this.master;
    }

    @Override
    public String getServiceName() {
        if (this.brokerController != null && this.brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopBufferMergeService.class.getSimpleName();
        }
        return PopBufferMergeService.class.getSimpleName();
    }

    @Override
    public void run() {
        // scan
        while (!this.isStopped()) {
            try {
                if (!isShouldRunning()) {
                    // slave
                    this.waitForRunning(interval * 200 * 5);
                    POP_LOGGER.info("Broker is {}, {}, clear all data",
                        brokerController.getMessageStoreConfig().getBrokerRole(), this.master);
                    this.buffer.clear();
                    this.commitOffsets.clear();
                    continue;
                }

                scan();
                if (scanTimes % countOfSecond30 == 0) {
                    scanGarbage();
                }

                this.waitForRunning(interval);

                if (!this.serving && this.buffer.size() == 0 && getOffsetTotalSize() == 0) {
                    this.serving = true;
                }
            } catch (Throwable e) {
                POP_LOGGER.error("PopBufferMergeService error", e);
                this.waitForRunning(3000);
            }
        }

        this.serving = false;
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }
        if (!isShouldRunning()) {
            return;
        }
        while (this.buffer.size() > 0 || getOffsetTotalSize() > 0) {
            scan();
        }
    }

    // 扫描已经完成的 CheckPoint，为它们提交消息消费进度
    private int scanCommitOffset() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.commitOffsets.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            // Key为topic@cid@queueId
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();
            PopCheckPointWrapper pointWrapper;
            while ((pointWrapper = queue.peek()) != null) {
                // 1. just offset & stored, not processed by scan
                // 2. ck is buffer(acked)
                // 3. ck is buffer(not all acked), all ak are stored and ck is stored
                if (pointWrapper.isJustOffset() && pointWrapper.isCkStored() // 已经将CK存到ReviveTopic
                    || isCkDone(pointWrapper)
                    || isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()
                ) {
                    if (commitOffset(pointWrapper)) { // 提交原生Topic队列偏移量
                        queue.poll(); // 将CK从内存中移除， CK在内存中一份存在bufffer中，一份存在queue中
                    } else {
                        break;
                    }
                } else {
                    if (System.currentTimeMillis() - pointWrapper.getCk().getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2L) {
                        POP_LOGGER.warn("[PopBuffer] ck offset long time not commit, {}", pointWrapper);
                    }
                    break;
                }
            }
            final int qs = queue.size();
            count += qs;
            if (qs > 5000 && scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer] offset queue size too long, {}, {}",
                    entry.getKey(), qs);
            }
        }
        return count;
    }

    public long getLatestOffset(String lockKey) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(lockKey);
        if (queue == null) {
            return -1;
        }
        PopCheckPointWrapper pointWrapper = queue.get().peekLast();
        if (pointWrapper != null) {
            return pointWrapper.getNextBeginOffset();
        }
        return -1;
    }

    public long getLatestOffset(String topic, String group, int queueId) {
        return getLatestOffset(KeyBuilder.buildPollingKey(topic, group, queueId));
    }

    private void scanGarbage() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = commitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            if (entry.getKey() == null) {
                continue;
            }
            String[] keyArray = entry.getKey().split(PopAckConstants.SPLIT);
            if (keyArray.length != 3) {
                continue;
            }
            String topic = keyArray[0];
            String cid = keyArray[1];
            // Topic已经不存在
            if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.info("[PopBuffer]remove not exit topic {} in buffer!", topic);
                iterator.remove();
                continue;
            }
            // 订阅组已经不存在
            if (!brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().containsKey(cid)) {
                POP_LOGGER.info("[PopBuffer]remove not exit sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
                continue;
            }
            if (System.currentTimeMillis() - entry.getValue().getTime() > minute5) {
                POP_LOGGER.info("[PopBuffer]remove long time not used sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
            }
        }
    }

    // 扫描内存中的CheckPoint,把已经匹配或存盘的CheckPoint移出buffer,把已经全部Ack的CheckPoint存盘
    private void scan() {
        long startTime = System.currentTimeMillis();
        int count = 0, countCk = 0;
        // Topic+CID+QueueId+StartOffset+PopTime+brokerName
        Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator = buffer.entrySet().iterator();
        // 遍历所有内存中的 CheckPoint
        while (iterator.hasNext()) {
            Map.Entry<String, PopCheckPointWrapper> entry = iterator.next();
            // POPCheckPoint
            PopCheckPointWrapper pointWrapper = entry.getValue();

            // 如果 CheckPoint 已经在磁盘中，或者全部消息都匹配成功，从内存中 buffer 中移除
            // just process offset(already stored at pull thread), or buffer ck(not stored and ack finish)
            if (pointWrapper.isJustOffset() && pointWrapper.isCkStored() // 如果以及在磁盘中且只是Offset了
                || isCkDone(pointWrapper) // 已经全部被ACK
                || isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored() // 全部在磁盘且已经ACK了。
            ) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("[PopBuffer]ck done, {}", pointWrapper);
                }
                iterator.remove();
                counter.decrementAndGet();
                continue;
            }

            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();

            // 是否要从内存中移除 CheckPoint
            boolean removeCk = !this.serving;
            // ck will be timeout
            // 默认3S 重放的时间为(POPTime+消息不可见时间) - 当前时间 < 3S
            if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut()) {
                removeCk = true;
            }

            // 在内存中时间大于阈值（默认10s）
            // the time stayed is too long
            // 当前时间-消息POP的时间 > 10S
            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime()) {
                removeCk = true;
            }

            // 当前时间 - 消息POP的时间 > 20S
            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2L) {
                POP_LOGGER.warn("[PopBuffer]ck finish fail, stay too long, {}", pointWrapper);
            }

            // double check
            if (isCkDone(pointWrapper)) { // 再次检查如果CheckPoint已经全部ACK
                continue;
            } else if (pointWrapper.isJustOffset()) {
                // just offset should be in store.
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    putCkToStore(pointWrapper, false);
                    countCk++;
                }
            } else if (removeCk) {
                // 将 CheckPoint 包装成消息放入磁盘，从内存中移除
                // put buffer ak to store
                // ReviveQueueOffset为-1的话，表示还没有放到磁盘中
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    putCkToStore(pointWrapper, false);
                    countCk++;
                }

                if (!pointWrapper.isCkStored()) {
                    continue;
                }

                // 在内存中移除 CheckPoint 前，把其中已经 Ack 的消息也作为 Ack 消息存入磁盘
                for (byte i = 0; i < point.getNum(); i++) {
                    // 遍历 CheckPoint 中消息 bit 码表每一位，检查是否已经 Ack 但是没有存到磁盘里面。
                    if (DataConverter.getBit(pointWrapper.getBits().get(), i) && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                        // 将ACK消息放到磁盘中
                        if (putAckToStore(pointWrapper, i)) {
                            count++;
                            // 标记ACK消息存入磁盘
                            markBitCAS(pointWrapper.getToStoreBits(), i);
                        }
                    }
                }
                // 如果ACK过的消息全部已存入了磁盘，将CheckPoint从内存当中移除
                if (isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("[PopBuffer]ck finish, {}", pointWrapper);
                    }
                    iterator.remove();
                    counter.decrementAndGet();
                }
            }
        }

        // 扫描已经完成的 CheckPoint，为它们提交消息消费进度
        int offsetBufferSize = scanCommitOffset();

        long eclipse = System.currentTimeMillis() - startTime;
        if (eclipse > brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() - 1000) {
            POP_LOGGER.warn("[PopBuffer]scan stop, because eclipse too long, PopBufferEclipse={}, " +
                    "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                eclipse, count, countCk, counter.get(), offsetBufferSize);
            this.serving = false;
        } else {
            if (scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer]scan, PopBufferEclipse={}, " +
                        "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                    eclipse, count, countCk, counter.get(), offsetBufferSize);
            }
        }
        PopMetricsManager.recordPopBufferScanTimeConsume(eclipse);
        scanTimes++;

        if (scanTimes >= countOfMinute1) {
            counter.set(this.buffer.size());
            scanTimes = 0;
        }
    }

    public int getOffsetTotalSize() {
        int count = 0;
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.commitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();
            count += queue.size();
        }
        return count;
    }

    public int getBufferedCKSize() {
        return this.counter.get();
    }

    private void markBitCAS(AtomicInteger setBits, int index) {
        while (true) {
            int bits = setBits.get();
            if (DataConverter.getBit(bits, index)) {
                break;
            }

            int newBits = DataConverter.setBit(bits, index, true);
            if (setBits.compareAndSet(bits, newBits)) {
                break;
            }
        }
    }

    private boolean commitOffset(final PopCheckPointWrapper wrapper) {
        if (wrapper.getNextBeginOffset() < 0) {
            return true;
        }

        final PopCheckPoint popCheckPoint = wrapper.getCk();
        final String lockKey = wrapper.getLockKey();

        if (!queueLockManager.tryLock(lockKey)) {
            return false;
        }
        try {
            final long offset = brokerController.getConsumerOffsetManager().queryOffset(popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId());
            if (wrapper.getNextBeginOffset() > offset) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("Commit offset, {}, {}", wrapper, offset);
                }
            } else {
                // maybe store offset is not correct.
                POP_LOGGER.warn("Commit offset, consumer offset less than store, {}, {}", wrapper, offset);
            }
            brokerController.getConsumerOffsetManager().commitOffset(getServiceName(),
                popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId(), wrapper.getNextBeginOffset());
        } finally {
            queueLockManager.unLock(lockKey);
        }
        return true;
    }

    private boolean putOffsetQueue(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());
        if (queue == null) {
            queue = new QueueWithTime<>();
            QueueWithTime old = this.commitOffsets.putIfAbsent(pointWrapper.getLockKey(), queue);
            if (old != null) {
                queue = old;
            }
        }
        queue.setTime(pointWrapper.getCk().getPopTime());
        return queue.get().offer(pointWrapper);
    }

    private boolean checkQueueOk(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());
        if (queue == null) {
            return true;
        }
        return queue.get().size() < brokerController.getBrokerConfig().getPopCkOffsetMaxQueueSize();
    }

    /**
     * put to store && add to buffer.
     *
     * @param point
     * @param reviveQueueId
     * @param reviveQueueOffset
     * @param nextBeginOffset
     * @return
     */
    public void addCkJustOffset(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset, long nextBeginOffset) {
        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset, true);

        // 将CheckPoint消息放到ReviveTopic中.
        this.putCkToStore(pointWrapper, !checkQueueOk(pointWrapper));

        // 将CheckPoint放到队列中
        putOffsetQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, {}", pointWrapper);
        }
    }

    // 添加空的 CheckPoint 到磁盘和内存
    public void addCkMock(String group, String topic, int queueId, long startOffset, long invisibleTime,
        long popTime, int reviveQueueId, long nextBeginOffset, String brokerName) {
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) 0);
        ck.setPopTime(popTime);
        ck.setInvisibleTime(invisibleTime);
        ck.setStartOffset(startOffset);
        ck.setCId(group);
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);

        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, Long.MAX_VALUE, ck, nextBeginOffset, true);
        pointWrapper.setCkStored(true);

        putOffsetQueue(pointWrapper);
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, mocked, {}", pointWrapper);
        }
    }

    // POP消息后新增 CheckPoint，放入内存 Buffer
    public boolean addCk(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset, long nextBeginOffset) {
        // key: point.getT() + point.getC() + point.getQ() + point.getSo() + point.getPt()
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        // 内存匹配服务是否开启
        if (!serving) {
            return false;
        }

        long now = System.currentTimeMillis();
        // 距离下次可重试 Pop 消费的时刻 < 4.5s, 默认3s
        // 通常是invisibleTime设置的值太小了，放到内存中没有意义，很小的概率被ACK掉。
        if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.warn("[PopBuffer]add ck, timeout, {}, {}", point, now);
            }
            return false;
        }

        if (this.counter.get() > brokerController.getBrokerConfig().getPopCkMaxBufferSize()) {
            POP_LOGGER.warn("[PopBuffer]add ck, max size, {}, {}", point, this.counter.get());
            return false;
        }

        // 构建POP检查点装饰器
        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset);

        if (!checkQueueOk(pointWrapper)) {
            return false;
        }

        // 将 CheckPoint 放入 Offset 队列
        putOffsetQueue(pointWrapper);
        // 将 CheckPoint 放入内存Buffer里面.
        // MergeKey为 Topic+CID+QueueId+StartOffset+PopTime+brokerName
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck, {}", pointWrapper);
        }
        return true;
    }

    // 消息 ACK，与内存中的 CheckPoint 匹配
    public boolean addAk(int reviveQid, AckMsg ackMsg) {
        // 如果未开启内存匹配，直接返回
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }
        try {
            // 根据 ACK 的消息找到内存 Buffer 中的 CheckPoint
            PopCheckPointWrapper pointWrapper = this.buffer.get(ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime() + ackMsg.getBrokerName());
            // 找不到 CheckPoint
            if (pointWrapper == null) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, no ck, {}", reviveQid, ackMsg);
                }
                return false;
            }

            // 内存中仅保存 Offset，实际已经保存到磁盘，内存中不处理 ACK 消息的匹配，直接返回
            // JustOffset，说明只是把这个CheckPoint当成索引使用，不会再在内存中匹配了，即不会走下面的逻辑。
            // 防止那种不可见时间很短的消息，走下面的逻辑，无效的去匹配一次，影响性能。
            if (pointWrapper.isJustOffset()) {
                return false;
            }

            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();

            if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, almost timeout for revive, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() - 1500) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, stay too long, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            // ACK CheckPoint 中的消息
            int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
            if (indexOfAck > -1) {
                // 设置CheckPoint中被Ack消息的bit码表为1
                markBitCAS(pointWrapper.getBits(), indexOfAck);
            } else {
                POP_LOGGER.error("[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}", reviveQid, ackMsg, point);
                return true;
            }

            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("[PopBuffer]add ack, rqId={}, {}, {}", reviveQid, pointWrapper, ackMsg);
            }

//            // check ak done
//            if (isCkDone(pointWrapper)) {
//                // cancel ck for timer
//                cancelCkTimer(pointWrapper);
//            }
            return true;
        } catch (Throwable e) {
            POP_LOGGER.error("[PopBuffer]add ack error, rqId=" + reviveQid + ", " + ackMsg, e);
        }

        return false;
    }

    public void clearOffsetQueue(String lockKey) {
        this.commitOffsets.remove(lockKey);
    }

    //将CheckPoint设置为定时消息，后投递到reviveTopic中。
    private void putCkToStore(final PopCheckPointWrapper pointWrapper, final boolean runInCurrent) {
        if (pointWrapper.getReviveQueueOffset() >= 0) {
            return;
        }
        // 构建CheckPoint为Message放到REVIVE_TOPIC中去，且会放到指定的队列中
        MessageExtBrokerInner msgInner = popMessageProcessor.buildCkMsg(pointWrapper.getCk(), pointWrapper.getReviveQueueId());
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        PopMetricsManager.incPopReviveCkPutCount(pointWrapper.getCk(), putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put ck to store fail: {}, {}", pointWrapper, putMessageResult);
            return;
        }
        pointWrapper.setCkStored(true);

        if (putMessageResult.isRemotePut()) {
            //No AppendMessageResult when escaping remotely
            pointWrapper.setReviveQueueOffset(0);
        } else {
            pointWrapper.setReviveQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
        }

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ck to store ok: {}, {}", pointWrapper, putMessageResult);
        }
    }

    private boolean putAckToStore(final PopCheckPointWrapper pointWrapper, byte msgIndex) {
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final AckMsg ackMsg = new AckMsg();

        ackMsg.setAckOffset(point.ackOffsetByIndex(msgIndex));
        ackMsg.setStartOffset(point.getStartOffset());
        ackMsg.setConsumerGroup(point.getCId());
        ackMsg.setTopic(point.getTopic());
        ackMsg.setQueueId(point.getQueueId());
        ackMsg.setPopTime(point.getPopTime());
        msgInner.setTopic(popMessageProcessor.reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
            return false;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ack to store ok: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
        }

        return true;
    }

    private boolean cancelCkTimer(final PopCheckPointWrapper pointWrapper) {
        // not stored, no need cancel
        if (pointWrapper.getReviveQueueOffset() < 0) {
            return true;
        }
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(popMessageProcessor.reviveTopic);
        msgInner.setBody((pointWrapper.getReviveQueueId() + "-" + pointWrapper.getReviveQueueOffset()).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());

        msgInner.setDeliverTimeMs(point.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]PutMessageCallback cancelCheckPoint fail, {}, {}", pointWrapper, putMessageResult);
            return false;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]cancelCheckPoint, {}", pointWrapper);
        }
        return true;
    }

    // 返回true表示码表的数量==消息的数量
    // 返回false表示码表的数量 < 消息的数量
    private boolean isCkDone(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        int bits = pointWrapper.getBits().get();
        for (byte i = 0; i < num; i++) {
            if (!DataConverter.getBit(bits, i)) {
                return false;
            }
        }
        return true;
    }

    // 表示所有的消息 == 已经ACK的数量(这部分不需要存盘) + 已存入Topic的数量(可能还未ACK)
    private boolean isCkDoneForFinish(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        // 异或运算
        int bits = pointWrapper.getBits().get() ^ pointWrapper.getToStoreBits().get();
        for (byte i = 0; i < num; i++) {
            if (DataConverter.getBit(bits, i)) {
                return false;
            }
        }
        return true;
    }

    public class QueueWithTime<T> {
        private final LinkedBlockingDeque<T> queue;
        private long time;

        public QueueWithTime() {
            this.queue = new LinkedBlockingDeque<>();
            this.time = System.currentTimeMillis();
        }

        public void setTime(long popTime) {
            this.time = popTime;
        }

        public long getTime() {
            return time;
        }

        public LinkedBlockingDeque<T> get() {
            return queue;
        }
    }

    public class PopCheckPointWrapper {
        private final int reviveQueueId;
        // -1: not stored, >=0: stored, Long.MAX: storing.
        private volatile long reviveQueueOffset;
        private final PopCheckPoint ck;
        // bit for concurrent
        private final AtomicInteger bits;
        // bit for stored buffer ak
        private final AtomicInteger toStoreBits;
        private final long nextBeginOffset;
        private final String lockKey;
        private final String mergeKey;
        private final boolean justOffset;
        private volatile boolean ckStored = false;

        public PopCheckPointWrapper(int reviveQueueId, long reviveQueueOffset, PopCheckPoint point,
            long nextBeginOffset) {
            this.reviveQueueId = reviveQueueId;
            this.reviveQueueOffset = reviveQueueOffset;
            this.ck = point;
            this.bits = new AtomicInteger(0);
            this.toStoreBits = new AtomicInteger(0);
            this.nextBeginOffset = nextBeginOffset;
            this.lockKey = ck.getTopic() + PopAckConstants.SPLIT + ck.getCId() + PopAckConstants.SPLIT + ck.getQueueId();
            this.mergeKey = point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName();
            this.justOffset = false;
        }

        public PopCheckPointWrapper(int reviveQueueId, long reviveQueueOffset, PopCheckPoint point,
            long nextBeginOffset,
            boolean justOffset) {
            this.reviveQueueId = reviveQueueId;
            this.reviveQueueOffset = reviveQueueOffset;
            this.ck = point;
            this.bits = new AtomicInteger(0);
            this.toStoreBits = new AtomicInteger(0);
            this.nextBeginOffset = nextBeginOffset;
            this.lockKey = ck.getTopic() + PopAckConstants.SPLIT + ck.getCId() + PopAckConstants.SPLIT + ck.getQueueId();
            this.mergeKey = point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName();
            this.justOffset = justOffset;
        }

        public int getReviveQueueId() {
            return reviveQueueId;
        }

        public long getReviveQueueOffset() {
            return reviveQueueOffset;
        }

        public boolean isCkStored() {
            return ckStored;
        }

        public void setReviveQueueOffset(long reviveQueueOffset) {
            this.reviveQueueOffset = reviveQueueOffset;
        }

        public PopCheckPoint getCk() {
            return ck;
        }

        public AtomicInteger getBits() {
            return bits;
        }

        public AtomicInteger getToStoreBits() {
            return toStoreBits;
        }

        public long getNextBeginOffset() {
            return nextBeginOffset;
        }

        public String getLockKey() {
            return lockKey;
        }

        public String getMergeKey() {
            return mergeKey;
        }

        public boolean isJustOffset() {
            return justOffset;
        }

        public void setCkStored(boolean ckStored) {
            this.ckStored = ckStored;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CkWrap{");
            sb.append("rq=").append(reviveQueueId);
            sb.append(", rqo=").append(reviveQueueOffset);
            sb.append(", ck=").append(ck);
            sb.append(", bits=").append(bits);
            sb.append(", sBits=").append(toStoreBits);
            sb.append(", nbo=").append(nextBeginOffset);
            sb.append(", cks=").append(ckStored);
            sb.append(", jo=").append(justOffset);
            sb.append('}');
            return sb.toString();
        }
    }

}
