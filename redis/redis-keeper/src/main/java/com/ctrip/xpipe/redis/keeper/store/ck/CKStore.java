package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.GtidTimestampItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.config.ConfigKeyListener;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.core.redis.operation.IRedisOpItem;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMultiItem;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.Keeperable;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

public class CKStore implements Keeperable {
    private static final Logger logger = LoggerFactory.getLogger(CKStore.class);

    // 使用 JCTools 的无锁单生产者单消费者队列
    private SpscArrayQueue<MessageEvent> queue;

    private Thread consumerThread;
    private volatile boolean running;
    private final AtomicBoolean consumerWaiting = new AtomicBoolean(false);
    private static final int BATCH_CONSUME_SIZE = 512;
    private static final int BATCH_NOTIFY_SIZE = 512;
    private final long replId;

    private final GtidTimestampAggregator timestampAggregator;

    private IOSupplier<GtidSet> executedGtidSetSupplier;

    private volatile LongSupplier secondsClock = () -> System.currentTimeMillis() / 1000;

    // 只在消费线程读写
    private long lastTimestampFlushMinute = -1;

    private static final String CK_BLOCK = "ck.block";
    private static final String CK_INIT = "ck.init";

    private KafkaService kafkaService;
    private MetricProxy metricProxy;

    private volatile boolean isKeeper;
    private RedisOpParser redisOpParser;
    private String address;

    private volatile boolean isSendCkFail;

    private volatile int offerCount = 0;

    private AtomicBoolean ckStarted = new AtomicBoolean(false);

    private static final ScheduledExecutorService hickwallReporterService =
            Executors.newSingleThreadScheduledExecutor();

    private ScheduledFuture<?> hickwallFuture;

    private ScheduledFuture<?> consumerFuture;

    private KeeperConfig keeperConfig;
    private volatile boolean started;
    private volatile boolean isReady;
    private ConfigKeyListener configKeyListener;

    public CKStore(ReplId replId, RedisOpParser redisOpParser, String address, KeeperConfig keeperConfig) {
        this.replId = replId != null ? replId.id() : -1;
        this.timestampAggregator = new GtidTimestampAggregator(this.replId);
        this.redisOpParser = redisOpParser;
        this.address = address;
        this.keeperConfig = keeperConfig;
    }

    // ========== 启动与停止 ==========

    public void start() {
        if(ckStarted.compareAndSet(false,true)) {
            metricProxy = MetricProxy.DEFAULT;
            kafkaService = KafkaService.DEFAULT;

            // 播种必须在队列为空、尚未接收事件时进行，否则会把积压事件所在旧分钟的 end_seq 写大
            seedTimestampWatermarks();
            startConsumerThread();

            isReady = true;

            configKeyListener = (key, val) -> {
                if (KeeperConfig.KEY_STOP_WRITE_CK.equals(key)) {
                    if ("true".equals(val)) {
                        isReady = false;
                        // 消费线程退出前会刷出剩余时间表数据
                        stopConsumerThread();
                        kafkaService.forceStopProducer();
                    } else {
                        // 新队列为空，可以安全播种
                        seedTimestampWatermarks();
                        startConsumerThread();
                        kafkaService.startProducer();
                        isReady = true;
                    }
                }
            };
            keeperConfig.addListener(configKeyListener);

            hickwallFuture = hickwallReporterService.scheduleWithFixedDelay(
                    () -> {
                        reportHickwall(CK_BLOCK, isSendCkFail);
                        reportHickwall(CK_INIT, !kafkaService.initSuccess());
                        isSendCkFail = false;
                    }, 1, 1, TimeUnit.MINUTES
            );

            consumerFuture = hickwallReporterService.scheduleWithFixedDelay(
                    () -> {
                        tryNotifyConsumer();
                    }, 1, 5, TimeUnit.SECONDS
            );
        }
    }

    private synchronized void startConsumerThread() {
        if (started) return;

        queue = new SpscArrayQueue<>(8192);
        running = true;
        consumerThread = new Thread(this::processEvents, "ck-consumer-" + replId);
        consumerThread.setDaemon(true);
        consumerThread.start();
        started = true;
    }

    private synchronized void stopConsumerThread() {
        if (!started) return;

        running = false;
        LockSupport.unpark(consumerThread); // 唤醒 park 中的消费者
        consumerThread.interrupt();
        try {
            consumerThread.join(2000);
        } catch (InterruptedException ignored) {
        }
        // 清空队列中的残留事件（避免内存泄漏）
        MessageEvent event;
        while ((event = queue.poll()) != null) {
            clearEvent(event);
        }
        started = false;
    }

    // ========== 消费者核心循环（批量处理） ==========
    private void processEvents() {
        List<MessageEvent> batch = new ArrayList<>(BATCH_CONSUME_SIZE);
        while (running) {
            // 每轮循环检查一次；空闲时依靠每 5 秒的定时唤醒回到这里，保证时间表按分钟刷出
            flushExpiredTimestampItemsIfNeeded();
            int drained = drainBatch(batch);
            if (drained > 0) {
                handleBatch(batch);
                batch.clear();
            } else {
                // 队列为空，准备进入等待
                consumerWaiting.set(true);
                // 二次检查，防止生产者刚好在 set(true) 后、park 前插入了数据
                if (drainBatch(batch) > 0) {
                    consumerWaiting.set(false);
                    handleBatch(batch);
                    batch.clear();
                    continue;
                }
                LockSupport.park();
                consumerWaiting.set(false);
            }
        }
        // 退出前清空剩余事件
        while (drainBatch(batch) > 0) {
            handleBatch(batch);
            batch.clear();
        }
        // 退出前刷出剩余时间表数据（包括当前分钟）
        flushAllTimestampItems();
    }

    /** 从队列中批量取出事件，返回实际取出的数量 */
    private int drainBatch(List<MessageEvent> batch) {
        for (int i = 0; i < BATCH_CONSUME_SIZE; i++) {
            MessageEvent event = queue.poll();
            if (event == null) return i;
            batch.add(event);
        }
        return BATCH_CONSUME_SIZE;
    }

    /** 批量处理事件（业务逻辑不变） */
    private void handleBatch(List<MessageEvent> batch) {
        for (MessageEvent event : batch) {
            try {
                IRedisOpItem item = event.getRedisOpItem();
                if (item == null) continue;
                long timestamp = event.getTimestamp();
                Object obj = item.getRedisOpItem();
                if (obj instanceof RedisOpItem) {
                    storeGtidWithKeyOrSubKeyItem((RedisOpItem) obj, timestamp);
                } else if (obj instanceof List) {
                    List<RedisOpItem> list = (List<RedisOpItem>) obj;
                    for (RedisOpItem op : list) {
                        try {
                            storeGtidWithKeyOrSubKeyItem(op, timestamp);
                        } catch (Exception e) {
                            logger.warn("[CKStore] process op error, opType={}", op.getRedisOpType(), e);
                        }
                    }
                }
            } catch (Exception e) {
                if(logger.isDebugEnabled()) {
                    logger.warn("[CKStore] handleEvent error", e);
                }
            } finally {
                clearEvent(event);
            }
        }
    }

    private void clearEvent(MessageEvent event) {
        IRedisOpItem item = event.getRedisOpItem();
        if (item != null) {
            try {
                item.clear();
            } catch (Exception e) {
                logger.warn("[CKStore] clear event error", e);
            }
        }
        event.setRedisOpItem(null);
        event.setTimestamp(0);
    }


    public void sendPayloads(List<RedisOpItem> redisOpItems) {
        if (checkStopWriteCk()) return;
        MessageEvent event = new MessageEvent();
        // 入队前只取一次时间，key 消息和时间表聚合都使用这个时间
        event.setTimestamp(secondsClock.getAsLong());
        event.setRedisOpItem(new RedisOpMultiItem(redisOpItems));

        if (!queue.offer(event)) {
            isSendCkFail = true;
        } else {
            onEventOffered();
        }
    }

    private void onEventOffered() {
        if (++offerCount >= BATCH_NOTIFY_SIZE) {
            offerCount = 0;
            tryNotifyConsumer();
        }
    }

    private void tryNotifyConsumer() {
        if (consumerWaiting.get()) {
            LockSupport.unpark(consumerThread);
        }
    }

    public boolean checkStopWriteCk() {
        return !isReady || keeperConfig.stopWriteCk();
    }

    // ------ 事务处理 ------
    private static final ThreadLocal<List<RedisOpItem>> TX_CONTEXT_ITEM = new ThreadLocal<>();

    public void storeGtidWithKeyOrSubKeyItem(RedisOpItem redisOpItem, long timestamp) {
        if (isNormalCommand(redisOpItem.getRedisOpType())) {
            processNormalCommandItem(redisOpItem, timestamp);
        } else {
            processTransactionCommandItem(redisOpItem, timestamp);
        }
    }

    private void processNormalCommandItem(RedisOpItem redisOp, long timestamp) {
        List<RedisOpItem> txOps = TX_CONTEXT_ITEM.get();
        if (txOps == null) {
            writeCKItem(redisOp.getGtid(), redisOp.getDbId(), redisOp, timestamp);
        } else {
            txOps.add(redisOp);
        }
    }

    private void processTransactionCommandItem(RedisOpItem redisOp, long timestamp) {
        switch (redisOp.getRedisOpType()) {
            case MULTI:
                TX_CONTEXT_ITEM.set(new ArrayList<>());
                break;
            case EXEC:
                commitTransactionItem(redisOp, timestamp);
                break;
        }
    }

    private void commitTransactionItem(RedisOpItem execOp, long timestamp) {
        List<RedisOpItem> ops = TX_CONTEXT_ITEM.get();
        if (ops != null) {
            try {
                if (!ops.isEmpty()) {
                    writeCKItem(execOp.getGtid(), execOp.getDbId(), ops, timestamp);
                }
            } finally {
                TX_CONTEXT_ITEM.remove();
            }
        }
    }

    private static boolean isNormalCommand(RedisOpType opType) {
        return opType != RedisOpType.MULTI && opType != RedisOpType.EXEC;
    }

    // ------ 写 Kafka ------
    public void writeCKItem(String gtid, String dbId, RedisOpItem redisOp, long timestamp) {
        if (StringUtil.isEmpty(gtid)) return;

        String uuid = gtid.substring(0, 40);
        String seq = gtid.substring(41);

        RedisKey redisKey = redisOp.getRedisKey();
        List<RedisKey> redisKeyList = redisOp.getRedisKeyList();
        boolean hasKeyMessage = redisKeyList == null ? redisKey != null : !redisKeyList.isEmpty();
        if (!hasKeyMessage) return;

        // seq 无法解析时只跳过时间表聚合，key 消息照常发送
        long seqValue = parseSeq(seq);
        if (seqValue >= 0) {
            timestampAggregator.update(uuid, seqValue, timestamp);
        }

        if (redisKey != null && redisKeyList == null) {
            kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(
                    redisOp.getRedisOpType().name(), uuid, seq,
                    redisKey.get(), null, dbId, replId, address, timestamp));
        } else if (redisKey != null) {
            for (RedisKey subKey : redisKeyList) {
                kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(
                        redisOp.getRedisOpType().name(), uuid, seq,
                        redisKey.get(), subKey.get(), dbId, replId, address, timestamp));
            }
        } else {
            for (RedisKey item : redisKeyList) {
                kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(
                        redisOp.getRedisOpType().name(), uuid, seq,
                        item.get(), null, dbId, replId, address, timestamp));
            }
        }
    }

    public void writeCKItem(String gtid, String dbId, List<RedisOpItem> redisOpList, long timestamp) {
        for (RedisOpItem redisOp : redisOpList) {
            writeCKItem(gtid, dbId, redisOp, timestamp);
        }
    }

    private void seedTimestampWatermarks() {
        if (executedGtidSetSupplier == null) return;

        try {
            timestampAggregator.seed(executedGtidSetSupplier.get());
        } catch (Throwable th) {
            logger.warn("[CKStore] seed timestamp watermarks fail", th);
        }
    }

    private static long parseSeq(String seq) {
        try {
            return Long.parseLong(seq);
        } catch (NumberFormatException e) {
            logger.debug("[CKStore] invalid seq {}, skip timestamp aggregation", seq);
            return -1;
        }
    }

    /** 只在消费线程调用：每分钟最多检查一次，发送所有已结束分钟的聚合数据 */
    private void flushExpiredTimestampItemsIfNeeded() {
        try {
            long now = secondsClock.getAsLong();
            long minute = GtidTimestampAggregator.alignToMinute(now);
            if (minute == lastTimestampFlushMinute) {
                return;
            }
            lastTimestampFlushMinute = minute;
            sendTimestampItems(timestampAggregator.drainExpired(now));
        } catch (Throwable th) {
            logger.warn("[CKStore] flush expired timestamp items fail", th);
        }
    }

    /** 只在消费线程退出前调用 */
    private void flushAllTimestampItems() {
        try {
            sendTimestampItems(timestampAggregator.drainAll());
        } catch (Throwable th) {
            logger.warn("[CKStore] flush all timestamp items fail", th);
        }
    }

    // 时间表消息丢失只会让 Console 定位的窗口变宽，因此发送失败不重试
    private void sendTimestampItems(List<GtidTimestampItem> timestampItems) {
        for (GtidTimestampItem timestampItem : timestampItems) {
            try {
                kafkaService.sendTimestampKafka(timestampItem);
            } catch (Throwable th) {
                logger.warn("[CKStore] send timestamp item fail, uuid={}, endSeq={}",
                        timestampItem.getUuid(), timestampItem.getEndSeq(), th);
            }
        }
    }

    private void reportHickwall(String type, boolean block) {
        if(block){
            CatEventMonitor.DEFAULT.logAlertEvent(type);
        }

        MetricData data = new MetricData(type);
        data.setValue(block ? 1 : 0);
        try {
            data.setTimestampMilli(System.currentTimeMillis());
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.warn("[xpipe][ck.reportHickwall]", e);
        }
    }

    public void dispose() {

        stopConsumerThread();

        if (keeperConfig != null && configKeyListener != null) {
            keeperConfig.removeListener(configKeyListener);
        }

        if(hickwallFuture != null) {
            hickwallFuture.cancel(false);
        }

        if(consumerFuture != null) {
            consumerFuture.cancel(false);
        }
    }

    @Override
    public boolean isKeeper() { return isKeeper; }

    @Override
    public void setKeeper() { this.isKeeper = true; }

    public void setMaster() { this.isKeeper = false; }

    /** 必须在 start() 之前设置，用于启动时播种累计水位 */
    public void setExecutedGtidSetSupplier(IOSupplier<GtidSet> executedGtidSetSupplier) {
        this.executedGtidSetSupplier = executedGtidSetSupplier;
    }

    @VisibleForTesting
    void setSecondsClock(LongSupplier secondsClock) {
        this.secondsClock = secondsClock;
    }

    public KeeperConfig getKeeperConfig() { return this.keeperConfig; }
}

