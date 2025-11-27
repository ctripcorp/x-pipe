package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.Keeperable;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

    public CKStore(ReplId replId, RedisOpParser redisOpParser){
        this.replId = replId != null ? replId.id() : -1;
        this.redisOpParser = redisOpParser;
    }

    public void start(){

        metricProxy = MetricProxy.DEFAULT;
        kafkaService = KafkaService.DEFAULT;
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
              for(RedisOpItem redisOpItem:event.getRedisOpItems()){
                  storeGtidWithKeyOrSubKeyItem(redisOpItem);
                  redisOpItem = null;
              }
              event.setRedisOpItems(null);
        });
        ringBuffer = disruptor.start();
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


    public void sendPayloads(List<Object[]> payloads) {
        if(ringBuffer == null) return;
        if(ringBuffer.hasAvailableCapacity(1)){
            long sequence = -1;
            try {
                sequence = ringBuffer.next();
                MessageEvent event = ringBuffer.get(sequence);
                List<RedisOpItem> redisOpItems = parsePayloads(payloads);
                event.setRedisOpItems(redisOpItems);
            } finally {
                payloads.clear();
                payloads = null;
                ringBuffer.publish(sequence);
            }
        }else {
            payloads.clear();
            payloads = null;
            reportHickwall(CK_BLOCK);
        }

    }

    private static final ThreadLocal<List<RedisOpItem>> TX_CONTEXT_ITEM = new ThreadLocal<>();

    private List<RedisOpItem> parsePayloads(List<Object[]> payloads){
        List<RedisOpItem> redisOpItems = new ArrayList<>(payloads.size());
        try {
            for (Object[] payload : payloads) {
                RedisOp redisOp = redisOpParser.parse(payload);
                RedisOpItem redisOpItem = new RedisOpItem();
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
                redisOpItems.add(redisOpItem);
            }
        }catch (Throwable th){
            logger.warn("[CKStore]parsePayloads{},error{}",payloads,th.getMessage());
        }
        return redisOpItems;
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
            kafkaService.sendKafka(new GtidKeyItem(redisOp.getRedisOpType().name(),gtidSeq[0],gtidSeq[1],redisKey.get(),null,dbId,replId));
        }else if(redisKey != null && redisKeyList != null){
            //包含子key的命令
            for(RedisKey subKey:redisKeyList) {
                kafkaService.sendKafka(new GtidKeyItem(redisOp.getRedisOpType().name(),gtidSeq[0],gtidSeq[1],redisKey.get(),subKey.get(),dbId,replId));
            }
        }else if(redisKey == null && redisKeyList != null){
            for(RedisKey item:redisKeyList){
                kafkaService.sendKafka(new GtidKeyItem(redisOp.getRedisOpType().name(),gtidSeq[0],gtidSeq[1],item.get(),null,dbId,replId));
            }
        }
    }

    public  void writeCKItem(String gtid,String dbId,List<RedisOpItem> redisOpList){
        for(RedisOpItem redisOp:redisOpList) {
            writeCKItem(gtid, dbId,redisOp);
        }
    }


    private void reportHickwall(String type) {
        MetricData data = new MetricData(type);
        data.setValue(1);
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
    }
}
