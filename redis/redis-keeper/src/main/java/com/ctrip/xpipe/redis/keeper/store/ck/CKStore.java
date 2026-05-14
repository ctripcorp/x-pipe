package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.config.ConfigKeyListener;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpMultiItem;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.Keeperable;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

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

    private static final String CK_BLOCK = "ck.block";

    private KafkaService kafkaService;
    private MetricProxy metricProxy;

    private volatile boolean isKeeper;
    private NioEventLoopGroup masterEventLoop;
    private RedisOpParser redisOpParser;
    private String address;

    private volatile boolean isSendCkFail;

    private volatile int offerCount = 0;


    private static final ScheduledExecutorService hickwallReporterService =
            Executors.newSingleThreadScheduledExecutor();

    private KeeperConfig keeperConfig;
    private volatile boolean started;
    private volatile boolean isReady;
    private ConfigKeyListener configKeyListener;

    public CKStore(ReplId replId, RedisOpParser redisOpParser, String address, KeeperConfig keeperConfig) {
        this.replId = replId != null ? replId.id() : -1;
        this.redisOpParser = redisOpParser;
        this.address = address;
        this.keeperConfig = keeperConfig;
    }

    // ========== 启动与停止 ==========

    public void start() {
        metricProxy = MetricProxy.DEFAULT;
        kafkaService = KafkaService.DEFAULT;

        startConsumerThread();

        isReady = true;

        configKeyListener = (key, val) -> {
            if (KeeperConfig.KEY_STOP_WRITE_CK.equals(key)) {
                if ("true".equals(val)) {
                    isReady = false;
                    stopConsumerThread();
                    kafkaService.forceStopProducer();
                } else {
                    startConsumerThread();
                    kafkaService.startProducer();
                    isReady = true;
                }
            }
        };
        keeperConfig.addListener(configKeyListener);

        hickwallReporterService.scheduleWithFixedDelay(
                () -> {
                    reportHickwall(CK_BLOCK, isSendCkFail);
                    isSendCkFail = false;
                }, 1, 1, TimeUnit.MINUTES
        );

        hickwallReporterService.scheduleWithFixedDelay(
                () -> {
                    tryNotifyConsumer();
                }, 1, 5, TimeUnit.SECONDS
        );
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
                Object obj = item.getRedisOpItem();
                if (obj instanceof RedisOpItem) {
                    storeGtidWithKeyOrSubKeyItem((RedisOpItem) obj);
                } else {
                    try {
                        List<RedisOpItem> list = (List<RedisOpItem>) obj;
                        for (RedisOpItem op : list) {
                            storeGtidWithKeyOrSubKeyItem(op);
                        }
                    }catch (Exception e){
                        logger.error("handleBatch",e);
                    }
                }
            } catch (Exception e) {
                logger.warn("[CKStore] handleEvent error", e);
            } finally {
                clearEvent(event);
            }
        }
    }

    private void clearEvent(MessageEvent event) {
        IRedisOpItem item = event.getRedisOpItem();
        if (item != null) {
            item.clear();
        }
        event.setRedisOpItem(null);
    }


    public void sendPayload(Object[] payload) {
        if (checkStopWriteCk()) return;

        MessageEvent event = new MessageEvent();
        RedisOpItem redisOpItem = parsePayload(payload);
        event.setRedisOpItem(redisOpItem);

        if (!queue.offer(event)) {
            // 队列满，丢弃
            event.setRedisOpItem(null);
            isSendCkFail = true;
        } else {
            onEventOffered();
        }
    }

    private void onEventOffered() {
        if (++offerCount >= BATCH_NOTIFY_SIZE) {
            offerCount = 0;                      // 重置计数器
            tryNotifyConsumer();
        }
    }

    /** 只有当消费者正处于等待状态时才唤醒它 */
    private void tryNotifyConsumer() {
        if (consumerWaiting.get()) {
            LockSupport.unpark(consumerThread);
        }
    }

    public void sendPayloads(List<RedisOpItem> redisOpItems) {
        if (checkStopWriteCk()) return;
        MessageEvent event = new MessageEvent();
        event.setRedisOpItem(new RedisOpMultiItem(redisOpItems));

        if (!queue.offer(event)) {
            // 队列满，丢弃
            isSendCkFail = true;
        } else {
            if (consumerWaiting.get()) {
                LockSupport.unpark(consumerThread);
            }
        }
    }

    public boolean checkStopWriteCk() {
        return !isReady || keeperConfig.stopWriteCk();
    }

    private List<com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem> parsePayloads(List<Object[]> payloads) {
        List<com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem> redisOpItems = new ArrayList<>(payloads.size());
        for (Object[] payload : payloads) {
            redisOpItems.add(parsePayload(payload));
        }
        return redisOpItems;
    }

    private RedisOpItem parsePayload(Object[] payload) {
        RedisOpItem redisOpItem = new RedisOpItem();
        try {
            RedisOp redisOp = redisOpParser.parse(payload);
            redisOpItem.setRedisOpType(redisOp.getOpType());
            redisOpItem.setGtid(redisOp.getOpGtid());
            redisOpItem.setDbId(redisOp.getDbId());
            if (redisOp instanceof RedisMultiKeyOp) {
                RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
                List<RedisKey> keys = redisMultiKeyOp.getKeys();
                redisOpItem.setRedisKeyList(keys);
            } else if (redisOp instanceof RedisMultiSubKeyOp) {
                RedisMultiSubKeyOp redisMultiSubKeyOp = (RedisMultiSubKeyOp) redisOp;
                RedisKey key = redisMultiSubKeyOp.getKey();
                List<RedisKey> subKeys = redisMultiSubKeyOp.getAllSubKeys();
                redisOpItem.setRedisKey(key);
                redisOpItem.setRedisKeyList(subKeys);
            } else if (redisOp instanceof RedisSingleKeyOp) {
                RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
                redisOpItem.setRedisKey(redisSingleKeyOp.getKey());
            }
        } catch (Throwable th) {
            logger.warn("[CKStore] parsePayload {}, error {}", payload, th.getMessage());
        }
        return redisOpItem;
    }

    // ------ 事务处理 ------
    private static final ThreadLocal<List<RedisOpItem>> TX_CONTEXT_ITEM = new ThreadLocal<>();

    public void storeGtidWithKeyOrSubKeyItem(RedisOpItem redisOpItem) {
        if (isNormalCommand(redisOpItem.getRedisOpType())) {
            processNormalCommandItem(redisOpItem);
        } else {
            processTransactionCommandItem(redisOpItem);
        }
    }

    private void processNormalCommandItem(RedisOpItem redisOp) {
        List<RedisOpItem> txOps = TX_CONTEXT_ITEM.get();
        if (txOps == null) {
            writeCKItem(redisOp.getGtid(), redisOp.getDbId(), redisOp);
        } else {
            txOps.add(redisOp);
        }
    }

    private void processTransactionCommandItem(RedisOpItem redisOp) {
        switch (redisOp.getRedisOpType()) {
            case MULTI:
                TX_CONTEXT_ITEM.set(new ArrayList<>());
                break;
            case EXEC:
                commitTransactionItem(redisOp);
                break;
        }
    }

    private void commitTransactionItem(RedisOpItem execOp) {
        List<RedisOpItem> ops = TX_CONTEXT_ITEM.get();
        if (ops != null) {
            try {
                if (!ops.isEmpty()) {
                    writeCKItem(execOp.getGtid(), execOp.getDbId(), ops);
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
    public void writeCKItem(String gtid, String dbId, RedisOpItem redisOp) {
        if (StringUtil.isEmpty(gtid)) return;

        String uuid = gtid.substring(0, 40);
        String seq = gtid.substring(41);

        RedisKey redisKey = redisOp.getRedisKey();
        List<RedisKey> redisKeyList = redisOp.getRedisKeyList();
        if (redisKey != null && redisKeyList == null) {
            kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(
                    redisOp.getRedisOpType().name(), uuid, seq,
                    redisKey.get(), null, dbId, replId, address));
        } else if (redisKey != null) {
            for (RedisKey subKey : redisKeyList) {
                kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(
                        redisOp.getRedisOpType().name(), uuid, seq,
                        redisKey.get(), subKey.get(), dbId, replId, address));
            }
        } else if (redisKeyList != null) {
            for (RedisKey item : redisKeyList) {
                kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(
                        redisOp.getRedisOpType().name(), uuid, seq,
                        item.get(), null, dbId, replId, address));
            }
        }
    }

    public void writeCKItem(String gtid, String dbId, List<RedisOpItem> redisOpList) {
        for (RedisOpItem redisOp : redisOpList) {
            writeCKItem(gtid, dbId, redisOp);
        }
    }

    private void reportHickwall(String type, boolean block) {
        MetricData data = new MetricData(type);
        data.setValue(block ? 1 : 0);
        data.addTag("replId", "" + replId);
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.warn("[xpipe][ck.reportHickwall]", e);
        }
    }

    public void dispose() {
        stopConsumerThread();
        if (kafkaService != null) kafkaService.forceStopProducer();
        if (keeperConfig != null && configKeyListener != null) {
            keeperConfig.removeListener(configKeyListener);
        }
    }

    @Override
    public boolean isKeeper() { return isKeeper; }

    @Override
    public void setKeeper() { this.isKeeper = true; }

    public void setMaster() { this.isKeeper = false; }

    public void setMasterEventLoop(NioEventLoopGroup masterEventLoop) {
        this.masterEventLoop = masterEventLoop;
    }

    public NioEventLoopGroup getMasterEventLoop() {
        return this.masterEventLoop;
    }

    public KeeperConfig getKeeperConfig() { return this.keeperConfig; }
}

