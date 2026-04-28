package com.ctrip.xpipe.redis.keeper.store.ck;


//public class CKStore implements Keeperable {
//    private static final Logger logger = LoggerFactory.getLogger(CKStore.class);
//
//    private Disruptor<MessageEvent> disruptor;
//
//    private RingBuffer<MessageEvent> ringBuffer;
//
//    private final long replId;
//
//    private static final String CK_BLOCK = "ck.block";
//
//    private KafkaService kafkaService;
//
//    private MetricProxy metricProxy;
//
//    private volatile boolean isKeeper;
//
//    private NioEventLoopGroup masterEventLoop;
//
//    private RedisOpParser redisOpParser;
//
//    private String address;
//
//    private volatile boolean isSendCkFail;
//
//    private static final ScheduledExecutorService hickwallReporterService = Executors.newSingleThreadScheduledExecutor();
//
//    private KeeperConfig keeperConfig;
//
//    private volatile boolean started;
//
//    private volatile boolean isReady;
//
//    private ConfigKeyListener configKeyListener;
//
//    public CKStore(ReplId replId, RedisOpParser redisOpParser,String address,KeeperConfig keeperConfig){
//        this.replId = replId != null ? replId.id() : -1;
//        this.redisOpParser = redisOpParser;
//        this.address = address;
//        this.keeperConfig = keeperConfig;
//    }
//
//    public void start(){
//
//        metricProxy = MetricProxy.DEFAULT;
//        kafkaService = KafkaService.DEFAULT;
//
//        startDisruptor();
//
//        isReady = true;
//
//        configKeyListener = (key, val) -> {
//            if(KeeperConfig.KEY_STOP_WRITE_CK.equals(key)){
//                if("true".equals(val)) {
//                    isReady = false;
//                    forceStopDisruptor();
//                    kafkaService.forceStopProducer();
//                }else {
//                    startDisruptor();
//                    kafkaService.startProducer();
//                    isReady = true;
//                }
//            }
//        };
//
//        keeperConfig.addListener(configKeyListener);
//
//        hickwallReporterService.scheduleWithFixedDelay(
//                () -> {
//                    reportHickwall(CK_BLOCK, isSendCkFail);
//                    isSendCkFail = false;
//                },
//                1,1, TimeUnit.MINUTES
//        );
//    }
//
//    private void startDisruptor(){
//        if(started) return;
//        MessageEventFactory factory = new MessageEventFactory();
//        int ringBufferSize = 1024; // must be a power of 2
//
//        disruptor = new Disruptor<>(
//                factory,                    // 事件工厂
//                ringBufferSize,             // RingBuffer大小
//                r -> {
//                    Thread thread = new Thread(r,  "disruptor-repl-" + replId);
//                    thread.setDaemon(true);
//                    return thread;
//                },
//                ProducerType.SINGLE,
//                new LiteBlockingWaitStrategy()
//        );
//
//        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
//            IRedisOpItem iRedisOpItem = event.getRedisOpItem();
//            Object item = iRedisOpItem.getRedisOpItem();
//            if(item instanceof RedisOpItem){
//                storeGtidWithKeyOrSubKeyItem((RedisOpItem) item);
//            }else {
//                List<RedisOpItem> redisOpItems = (List<RedisOpItem>) item;
//                for (RedisOpItem redisOpItem : redisOpItems) {
//                    storeGtidWithKeyOrSubKeyItem(redisOpItem);
//                }
//            }
//            iRedisOpItem.clear();
//            event.setRedisOpItem(null);
//        });
//        ringBuffer = disruptor.start();
//        started = true;
//    }
//
//    private void forceStopDisruptor(){
//        if(!started) return;
//        try {
//            if(disruptor != null) {
//                disruptor.halt();
//            }
//        }finally {
//            ringBuffer = null;
//            disruptor = null;
//            started = false;
//        }
//    }
//
//    public boolean isKeeper(){
//        return isKeeper;
//    }
//
//    public void setKeeper(){
//        this.isKeeper = true;
//    }
//
//    public void setMaster(){
//        this.isKeeper = false;
//    }
//
//    public void setMasterEventLoop(NioEventLoopGroup masterEventLoop){
//        this.masterEventLoop = masterEventLoop;
//    }
//
//    public NioEventLoopGroup getMasterEventLoop(){
//        return this.masterEventLoop;
//    }
//
//    public boolean checkStopWriteCk(){
//        if(!isReady || keeperConfig.stopWriteCk()) return true;
//        return false;
//    }
//
//    public void sendPayloads(List<Object[]> payloads) {
//        if(checkStopWriteCk()) return;
//        if(ringBuffer.hasAvailableCapacity(1)){
//            long sequence = -1;
//            try {
//                sequence = ringBuffer.next();
//                MessageEvent event = ringBuffer.get(sequence);
//                List<RedisOpItem> redisOpItems = parsePayloads(payloads);
//                event.setRedisOpItem(new RedisOpMultiItem(redisOpItems));
//            } finally {
//                ringBuffer.publish(sequence);
//                releasePayloads(payloads);
//            }
//        }else {
//            releasePayloads(payloads);
//            isSendCkFail = true;
//        }
//
//    }
//
//
//    public void sendPayload(Object[] payload) {
//        if(checkStopWriteCk()) return;
//        if(ringBuffer.hasAvailableCapacity(1)){
//            long sequence = -1;
//            try {
//                sequence = ringBuffer.next();
//                MessageEvent event = ringBuffer.get(sequence);
//                RedisOpItem redisOpItem = parsePayload(payload);
//                event.setRedisOpItem(redisOpItem);
//            } finally {
//                ringBuffer.publish(sequence);
//                releasePayload(payload);
//            }
//        }else {
//            releasePayload(payload);
//            isSendCkFail = true;
//        }
//
//    }
//
//
//    private void releasePayloads(List<Object[]> payloads){
//        for(Object[] payload:payloads){
//            releasePayload(payload);
//        }
//        payloads = null;
//    }
//
//    private void releasePayload(Object[] payload){
//        for(Object obj:payload){
//            if(obj instanceof ByteArrayOutputStreamPayload){
//                ByteArrayOutputStreamPayload byteArrayOutputStreamPayload = (ByteArrayOutputStreamPayload) obj;
//                byteArrayOutputStreamPayload.clear();
//            }
//            obj = null;
//        }
//        payload = null;
//    }
//
//    private static final ThreadLocal<List<RedisOpItem>> TX_CONTEXT_ITEM = new ThreadLocal<>();
//
//    private List<RedisOpItem> parsePayloads(List<Object[]> payloads){
//        List<RedisOpItem> redisOpItems = new ArrayList<>(payloads.size());
//        for(Object[] payload:payloads) {
//            RedisOpItem redisOpItem = parsePayload(payload);
//            redisOpItems.add(redisOpItem);
//        }
//        return redisOpItems;
//    }
//
//    private RedisOpItem parsePayload(Object[] payload){
//        RedisOpItem redisOpItem = new RedisOpItem();
//        try {
//                RedisOp redisOp = redisOpParser.parse(payload);
//                redisOpItem.setRedisOpType(redisOp.getOpType());
//                redisOpItem.setGtid(redisOp.getOpGtid());
//                redisOpItem.setDbId(redisOp.getDbId());
//                if (redisOp instanceof RedisMultiKeyOp) {
//                    RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
//                    List<RedisKey> keys = redisMultiKeyOp.getKeys();
//                    redisOpItem.setRedisKeyList(keys);
//                } else if (redisOp instanceof RedisMultiSubKeyOp) {
//                    RedisMultiSubKeyOp redisMultiSubKeyOp = (RedisMultiSubKeyOp) redisOp;
//                    RedisKey key = redisMultiSubKeyOp.getKey();
//                    List<RedisKey> subKeys = redisMultiSubKeyOp.getAllSubKeys();
//                    redisOpItem.setRedisKey(key);
//                    redisOpItem.setRedisKeyList(subKeys);
//                } else if (redisOp instanceof RedisSingleKeyOp) {
//                    RedisSingleKeyOp redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
//                    redisOpItem.setRedisKey(redisSingleKeyOp.getKey());
//                }
//        }catch (Throwable th){
//            logger.warn("[CKStore]parsePayload{},error{}",payload,th.getMessage());
//        }
//        return redisOpItem;
//    }
//
//
//    public void storeGtidWithKeyOrSubKeyItem(RedisOpItem redisOpItem) {
//
//        // 使用位运算快速判断是否为普通命令（假设MULTI和EXEC是少数特定值）
//        if (isNormalCommand(redisOpItem.getRedisOpType())) {
//            processNormalCommandItem(redisOpItem);
//        } else {
//            processTransactionCommandItem(redisOpItem);
//        }
//    }
//
//    private  void processNormalCommandItem(RedisOpItem redisOp) {
//        List<RedisOpItem> txOps = TX_CONTEXT_ITEM.get();
//        if (txOps == null) {
//            // 快速路径：不在事务中
//            writeCKItem(redisOp.getGtid(), redisOp.getDbId(), redisOp);
//        } else {
//            // 慢速路径：在事务中
//            txOps.add(redisOp);
//        }
//    }
//
//    private  void processTransactionCommandItem(RedisOpItem redisOp) {
//        switch (redisOp.getRedisOpType()) {
//            case MULTI:
//                TX_CONTEXT_ITEM.set(new ArrayList<>());
//                break;
//            case EXEC:
//                commitTransactionItem(redisOp);
//                break;
//        }
//    }
//
//    private  void commitTransactionItem(RedisOpItem execOp) {
//        List<RedisOpItem> ops = TX_CONTEXT_ITEM.get();
//        if (ops != null) {
//            try {
//                if (!ops.isEmpty()) {
//                    writeCKItem(execOp.getGtid(), execOp.getDbId(), ops);
//                }
//            }finally {
//                TX_CONTEXT_ITEM.remove();
//            }
//        }
//    }
//
//    // 快速判断是否为普通命令（非MULTI、非EXEC）
//    private static boolean isNormalCommand(RedisOpType opType) {
//        return opType != RedisOpType.MULTI && opType != RedisOpType.EXEC;
//    }
//
//    public void writeCKItem(String gtid,String dbId, RedisOpItem redisOp){
//        if(StringUtil.isEmpty(gtid)) return;
//
//        String uuid = gtid.substring(0,40);
//        String seq = gtid.substring(41,gtid.length());
//
//        RedisKey redisKey = redisOp.getRedisKey();
//        List<RedisKey> redisKeyList = redisOp.getRedisKeyList();
//        if(redisKey != null && redisKeyList == null) {
//            kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(redisOp.getRedisOpType().name(),uuid,seq,redisKey.get(),null,dbId,replId,address));
//        }else if(redisKey != null && redisKeyList != null){
//            //包含子key的命令
//            for(RedisKey subKey:redisKeyList) {
//                kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(redisOp.getRedisOpType().name(),uuid,seq,redisKey.get(),subKey.get(),dbId,replId,address));
//            }
//        }else if(redisKey == null && redisKeyList != null){
//            for(RedisKey item:redisKeyList){
//                kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(redisOp.getRedisOpType().name(),uuid,seq,item.get(),null,dbId,replId,address));
//            }
//        }
//    }
//
//
//    public  void writeCKItem(String gtid,String dbId,List<RedisOpItem> redisOpList){
//        for(RedisOpItem redisOp:redisOpList) {
//            writeCKItem(gtid, dbId,redisOp);
//        }
//    }
//
//
//    private void reportHickwall(String type, boolean block) {
//        MetricData data = new MetricData(type);
//        data.setValue(block ? 1 : 0);
//        data.addTag("replId", ""+replId);
//        data.setTimestampMilli(System.currentTimeMillis());
//        try {
//            metricProxy.writeBinMultiDataPoint(data);
//        } catch (Exception e) {
//            logger.warn("[xpipe][ck.reportHickwall]",e);
//        }
//    }
//
//    public void dispose() {
//        if (disruptor != null){
//            disruptor.shutdown();
//        }
//        if(keeperConfig != null) {
//            if(configKeyListener != null) {
//                keeperConfig.removeListener(configKeyListener);
//            }
//        }
//    }
//
//    public KeeperConfig getKeeperConfig(){
//        return this.keeperConfig;
//    }
//
//}

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.config.ConfigKeyListener;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.payload.DirectByteBufInStringOutPayload;
import com.ctrip.xpipe.redis.core.redis.operation.*;
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
                    tryNotifyConsumer();
                }, 1, 1, TimeUnit.MINUTES
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

    // ========== 消费者核心循环 ==========

//    private void processEvents() {
//        while (running) {
//            MessageEvent event = queue.poll();
//            if (event != null) {
//                handleEvent(event);
//            } else {
//                // 队列为空，准备 park
//                consumerWaiting.set(true);
//                // 二次检查，防止生产者在这期间插入数据但未唤醒
//                if (queue.poll() != null) {
//                    consumerWaiting.set(false);
//                    continue; // 重新进入循环处理
//                }
//                LockSupport.park();
//                consumerWaiting.set(false);
//            }
//        }
//        // 退出前处理剩余事件
//        MessageEvent event;
//        while ((event = queue.poll()) != null) {
//            handleEvent(event);
//        }
//    }

    private void handleEvent(MessageEvent event) {
        try {
            IRedisOpItem iRedisOpItem = event.getRedisOpItem();
            if (iRedisOpItem == null) return;
            Object item = iRedisOpItem.getRedisOpItem();
            if (item instanceof RedisOpItem) {
                storeGtidWithKeyOrSubKeyItem((RedisOpItem) item);
            } else {
                @SuppressWarnings("unchecked")
                List<RedisOpItem> redisOpItems = (List<RedisOpItem>) item;
                for (RedisOpItem redisOpItem : redisOpItems) {
                    storeGtidWithKeyOrSubKeyItem(redisOpItem);
                }
            }
        } catch (Exception e) {
            logger.warn("[CKStore] handleEvent error", e);
        } finally {
            clearEvent(event);
        }
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
                    @SuppressWarnings("unchecked")
                    List<RedisOpItem> list = (List<RedisOpItem>) obj;
                    for (RedisOpItem op : list) {
                        storeGtidWithKeyOrSubKeyItem(op);
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
        // MessageEvent 对象可复用（若需要），此处简单丢弃，GC 会回收
    }

    // ========== 生产者接口 ==========

    public void sendPayload(Object[] payload) {
        if (checkStopWriteCk()) return;

        MessageEvent event = new MessageEvent();
        RedisOpItem redisOpItem = parsePayload(payload);
        event.setRedisOpItem(redisOpItem);

        if (!queue.offer(event)) {
            // 队列满，丢弃
            event.setRedisOpItem(null);
            releasePayload(payload);
            isSendCkFail = true;
        } else {
            onEventOffered();
            releasePayload(payload);
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

    public void sendPayloads(List<Object[]> payloads) {
        if (checkStopWriteCk()) return;

        MessageEvent event = new MessageEvent();
        List<RedisOpItem> redisOpItems = parsePayloads(payloads);
        event.setRedisOpItem(new RedisOpMultiItem(redisOpItems));

        if (!queue.offer(event)) {
            // 队列满，丢弃
            releasePayloads(payloads);
            isSendCkFail = true;
        } else {
            if (consumerWaiting.get()) {
                LockSupport.unpark(consumerThread);
            }
            releasePayloads(payloads);
        }
    }

    // ========== 业务逻辑（原样保留） ==========

    public boolean checkStopWriteCk() {
        return !isReady || keeperConfig.stopWriteCk();
    }

    // ------ 解析与释放 ------
    private void releasePayloads(List<Object[]> payloads) {
        for (Object[] payload : payloads) {
            releasePayload(payload);
        }
    }

    private void releasePayload(Object[] payload) {
        for (Object obj : payload) {
            if (obj instanceof ByteArrayOutputStreamPayload) {
                ((ByteArrayOutputStreamPayload) obj).clear();
            }
        }
    }

    private List<RedisOpItem> parsePayloads(List<Object[]> payloads) {
        List<RedisOpItem> redisOpItems = new ArrayList<>(payloads.size());
        for (Object[] payload : payloads) {
            redisOpItems.add(parsePayload(payload));
        }
        return redisOpItems;
    }

    private RedisOpItem parseByteBufPayload(Object[] payload) {
        RedisOpItem redisOpItem = new RedisOpItem();
        try {
            DirectByteBufInStringOutPayload gtid = (DirectByteBufInStringOutPayload) payload[1];
            DirectByteBufInStringOutPayload dbId = (DirectByteBufInStringOutPayload) payload[2];
            DirectByteBufInStringOutPayload cmd = (DirectByteBufInStringOutPayload) payload[3];
            DirectByteBufInStringOutPayload key = (DirectByteBufInStringOutPayload) payload[4];
            redisOpItem.setRedisOpType(RedisOpType.valueOf(cmd.toString().toUpperCase()));
            redisOpItem.setGtid(gtid.toString());
            redisOpItem.setDbId(dbId.toString());
            redisOpItem.setRedisKey(new RedisKey(key.getBytes()));

        } catch (Throwable th) {
            logger.warn("[CKStore] parsePayload {}, error {}", payload, th.getMessage());
        }
        return redisOpItem;
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
        } else if (redisKey != null && redisKeyList != null) {
            for (RedisKey subKey : redisKeyList) {
                kafkaService.sendKafka(GtidKeyItem.buildGtidKeyItem(
                        redisOp.getRedisOpType().name(), uuid, seq,
                        redisKey.get(), subKey.get(), dbId, replId, address));
            }
        } else if (redisKey == null && redisKeyList != null) {
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

    // ========== 其他辅助 ==========

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

    // ------ Keeperable 接口方法 ------
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

