package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.config.ConfigKeyListener;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.Keeperable;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.utils.StringUtil;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CKStore implements Keeperable {
    private static final Logger logger = LoggerFactory.getLogger(CKStore.class);

    private Disruptor<MessageEvent> disruptor;

    private RingBuffer<MessageEvent> ringBuffer;

    private final long replId;

    private static final String CK_BLOCK = "ck.block";

    private KafkaService kafkaService;

    private MetricProxy metricProxy;

    private volatile boolean isKeeper;

    private RedisOpParser redisOpParser;

    private String address;

    private volatile boolean isSendCkFail;

    private static final ScheduledExecutorService hickwallReporterService = Executors.newSingleThreadScheduledExecutor();

    private KeeperConfig keeperConfig;

    private volatile boolean started;

    private volatile boolean isReady;

    private ConfigKeyListener configKeyListener;

    public CKStore(ReplId replId, RedisOpParser redisOpParser,String address,KeeperConfig keeperConfig){
        this.replId = replId != null ? replId.id() : -1;
        this.redisOpParser = redisOpParser;
        this.address = address;
        this.keeperConfig = keeperConfig;
    }

    public void start(){

        metricProxy = MetricProxy.DEFAULT;
        kafkaService = KafkaService.DEFAULT;

        startDisruptor();

        isReady = true;

        configKeyListener = (key, val) -> {
            if(KeeperConfig.KEY_STOP_WRITE_CK.equals(key)){
                if("true".equals(val)) {
                    isReady = false;
                    forceStopDisruptor();
                    kafkaService.forceStopProducer();
                }else {
                    startDisruptor();
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
                },
                1,1, TimeUnit.MINUTES
        );
    }

    private void startDisruptor(){
        if(started) return;
        MessageEventFactory factory = new MessageEventFactory();
        int ringBufferSize = 1024; // must be a power of 2

        disruptor = new Disruptor<>(
                factory,                    // 事件工厂
                ringBufferSize,             // RingBuffer大小
                r -> {
                    Thread thread = new Thread(r,  "disruptor-repl-" + replId);
                    thread.setDaemon(true);
                    return thread;
                },
                ProducerType.SINGLE,
                new LiteBlockingWaitStrategy()
        );

        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            IRedisOpItem iRedisOpItem = event.getRedisOpItem();
            Object item = iRedisOpItem.getRedisOpItem();
            if(item instanceof RedisOpItem){
                storeGtidWithKeyOrSubKeyItem((RedisOpItem) item);
            }else {
                List<RedisOpItem> redisOpItems = (List<RedisOpItem>) item;
                for (RedisOpItem redisOpItem : redisOpItems) {
                    storeGtidWithKeyOrSubKeyItem(redisOpItem);
                }
            }
            iRedisOpItem.clear();
            event.setRedisOpItem(null);
        });
        ringBuffer = disruptor.start();
        started = true;
    }

    private void forceStopDisruptor(){
        if(!started) return;
        try {
            if(disruptor != null) {
                disruptor.halt();
            }
        }finally {
            ringBuffer = null;
            disruptor = null;
            started = false;
        }
    }

    public boolean isKeeper(){
        return isKeeper;
    }

    public void setKeeper(){
        this.isKeeper = true;
    }

    public void setMaster(){
        this.isKeeper = false;
    }

    public boolean checkStopWriteCk(){
        if(!isReady || keeperConfig.stopWriteCk()) return true;
        return false;
    }

    public void sendPayloads(List<Object[]> payloads) {
        if(checkStopWriteCk()) return;
        if(ringBuffer.hasAvailableCapacity(1)){
            long sequence = -1;
            try {
                sequence = ringBuffer.next();
                MessageEvent event = ringBuffer.get(sequence);
                List<RedisOpItem> redisOpItems = parsePayloads(payloads);
                event.setRedisOpItem(new RedisOpMultiItem(redisOpItems));
            } finally {
                ringBuffer.publish(sequence);
                releasePayloads(payloads);
            }
        }else {
            releasePayloads(payloads);
            isSendCkFail = true;
        }

    }


    public void sendPayload(Object[] payload) {
        if(checkStopWriteCk()) return;
        if(ringBuffer.hasAvailableCapacity(1)){
            long sequence = -1;
            try {
                sequence = ringBuffer.next();
                MessageEvent event = ringBuffer.get(sequence);
                RedisOpItem redisOpItem = parsePayload(payload);
                event.setRedisOpItem(redisOpItem);
            } finally {
                ringBuffer.publish(sequence);
                releasePayload(payload);
            }
        }else {
            releasePayload(payload);
            isSendCkFail = true;
        }

    }


    private void releasePayloads(List<Object[]> payloads){
        for(Object[] payload:payloads){
            releasePayload(payload);
        }
        payloads = null;
    }

    private void releasePayload(Object[] payload){
        for(Object obj:payload){
            if(obj instanceof ByteArrayOutputStreamPayload){
                ByteArrayOutputStreamPayload byteArrayOutputStreamPayload = (ByteArrayOutputStreamPayload) obj;
                byteArrayOutputStreamPayload.clear();
            }
            obj = null;
        }
        payload = null;
    }

    private static final ThreadLocal<List<RedisOpItem>> TX_CONTEXT_ITEM = new ThreadLocal<>();

    private List<RedisOpItem> parsePayloads(List<Object[]> payloads){
        List<RedisOpItem> redisOpItems = new ArrayList<>(payloads.size());
        for(Object[] payload:payloads) {
            RedisOpItem redisOpItem = parsePayload(payload);
            redisOpItems.add(redisOpItem);
        }
        return redisOpItems;
    }

    private RedisOpItem parsePayload(Object[] payload){
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
        }catch (Throwable th){
            logger.warn("[CKStore]parsePayload{},error{}",payload,th.getMessage());
        }
        return redisOpItem;
    }


    public void storeGtidWithKeyOrSubKeyItem(RedisOpItem redisOpItem) {

        // 使用位运算快速判断是否为普通命令（假设MULTI和EXEC是少数特定值）
        if (isNormalCommand(redisOpItem.getRedisOpType())) {
            processNormalCommandItem(redisOpItem);
        } else {
            processTransactionCommandItem(redisOpItem);
        }
    }

    private  void processNormalCommandItem(RedisOpItem redisOp) {
        List<RedisOpItem> txOps = TX_CONTEXT_ITEM.get();
        if (txOps == null) {
            // 快速路径：不在事务中
            writeCKItem(redisOp.getGtid(), redisOp.getDbId(), redisOp);
        } else {
            // 慢速路径：在事务中
            txOps.add(redisOp);
        }
    }

    private  void processTransactionCommandItem(RedisOpItem redisOp) {
        switch (redisOp.getRedisOpType()) {
            case MULTI:
                TX_CONTEXT_ITEM.set(new ArrayList<>());
                break;
            case EXEC:
                commitTransactionItem(redisOp);
                break;
        }
    }

    private  void commitTransactionItem(RedisOpItem execOp) {
        List<RedisOpItem> ops = TX_CONTEXT_ITEM.get();
        if (ops != null) {
            try {
                if (!ops.isEmpty()) {
                    writeCKItem(execOp.getGtid(), execOp.getDbId(), ops);
                }
            }finally {
                TX_CONTEXT_ITEM.remove();
            }
        }
    }

    // 快速判断是否为普通命令（非MULTI、非EXEC）
    private static boolean isNormalCommand(RedisOpType opType) {
        return opType != RedisOpType.MULTI && opType != RedisOpType.EXEC;
    }

    public void writeCKItem(String gtid,String dbId, RedisOpItem redisOp){
        if(StringUtil.isEmpty(gtid)) return;
        String[] gtidSeq = gtid.split(":");

        RedisKey redisKey = redisOp.getRedisKey();
        List<RedisKey> redisKeyList = redisOp.getRedisKeyList();
        if(redisKey != null && redisKeyList == null) {
            kafkaService.sendKafka(new GtidKeyItem(redisOp.getRedisOpType().name(),gtidSeq[0],gtidSeq[1],redisKey.get(),null,dbId,replId,address));
        }else if(redisKey != null && redisKeyList != null){
            //包含子key的命令
            for(RedisKey subKey:redisKeyList) {
                kafkaService.sendKafka(new GtidKeyItem(redisOp.getRedisOpType().name(),gtidSeq[0],gtidSeq[1],redisKey.get(),subKey.get(),dbId,replId,address));
            }
        }else if(redisKey == null && redisKeyList != null){
            for(RedisKey item:redisKeyList){
                kafkaService.sendKafka(new GtidKeyItem(redisOp.getRedisOpType().name(),gtidSeq[0],gtidSeq[1],item.get(),null,dbId,replId,address));
            }
        }
    }

    public  void writeCKItem(String gtid,String dbId,List<RedisOpItem> redisOpList){
        for(RedisOpItem redisOp:redisOpList) {
            writeCKItem(gtid, dbId,redisOp);
        }
    }


    private void reportHickwall(String type, boolean block) {
        MetricData data = new MetricData(type);
        data.setValue(block ? 1 : 0);
        data.addTag("replId", ""+replId);
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Exception e) {
            logger.warn("[xpipe][ck.reportHickwall]",e);
        }
    }

    public void dispose() {
        if (disruptor != null){
            disruptor.shutdown();
        }
        if(keeperConfig != null) {
            if(configKeyListener != null) {
                keeperConfig.removeListener(configKeyListener);
            }
        }
    }
}
