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
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CKStore implements Keeperable {
    private static final Logger logger = LoggerFactory.getLogger(CKStore.class);

    private  final Disruptor<MessageEvent> disruptor;

    private  final RingBuffer<MessageEvent> ringBuffer;

    private final long replId;

    private static final String CK_BLOCK = "ck.block";

    private final KafkaService kafkaService;

    private final MetricProxy metricProxy;

    private volatile boolean isKeeper;

    public CKStore(ReplId replId, RedisOpParser redisOpParser){
        this.replId = replId != null ? replId.id() : -1;

        metricProxy = MetricProxy.DEFAULT;
        kafkaService = KafkaService.DEFAULT;

        MessageEventFactory factory = new MessageEventFactory();
        int ringBufferSize = 131072; // must be a power of 2

        disruptor = new Disruptor<>(
                factory,                    // 事件工厂
                ringBufferSize,             // RingBuffer大小
                DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE,
                new SleepingWaitStrategy()
        );

        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            for(Object[] payload: event.getPayloads()){
                storeGtidWithKeyOrSubKey(payload,redisOpParser);
            }
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
        if(ringBuffer.hasAvailableCapacity(1)){
            long sequence = -1;
            try {
                sequence = ringBuffer.next();
                MessageEvent event = ringBuffer.get(sequence);
                event.setPayloads(payloads);
            } finally {
                ringBuffer.publish(sequence);
            }
        }else {
            reportHickwall(CK_BLOCK);
        }

    }

    private static final ThreadLocal<List<RedisOp>> TX_CONTEXT = new ThreadLocal<>();

    public void storeGtidWithKeyOrSubKey(Object[] payload, RedisOpParser opParser) {
        RedisOp redisOp = opParser.parse(payload);

        // 使用位运算快速判断是否为普通命令（假设MULTI和EXEC是少数特定值）
        if (isNormalCommand(redisOp.getOpType())) {
            processNormalCommand(redisOp);
        } else {
            processTransactionCommand(redisOp);
        }
    }

    // 快速判断是否为普通命令（非MULTI、非EXEC）
    private static boolean isNormalCommand(RedisOpType opType) {
        return opType != RedisOpType.MULTI && opType != RedisOpType.EXEC;
    }

    private  void processNormalCommand(RedisOp redisOp) {
        List<RedisOp> txOps = TX_CONTEXT.get();
        if (txOps == null) {
            // 快速路径：不在事务中
            writeCK(redisOp.getOpGtid(), redisOp.getDbId(), redisOp);
        } else {
            // 慢速路径：在事务中
            txOps.add(redisOp);
        }
    }

    private  void processTransactionCommand(RedisOp redisOp) {
        switch (redisOp.getOpType()) {
            case MULTI:
                TX_CONTEXT.set(new ArrayList<>());
                break;
            case EXEC:
                commitTransaction(redisOp);
                break;
        }
    }

    private  void commitTransaction(RedisOp execOp) {
        List<RedisOp> ops = TX_CONTEXT.get();
        if (ops != null) {
            try {
                if (!ops.isEmpty()) {
                    writeCK(execOp.getOpGtid(), execOp.getDbId(), ops);
                }
            }finally {
                TX_CONTEXT.remove();
            }
        }
    }

    public void writeCK(String gtid,String dbId, RedisOp redisOp){
        if(StringUtil.isEmpty(gtid)) return;
        String[] gtidSeq = gtid.split(":");

        if(redisOp instanceof RedisMultiKeyOp){
            RedisMultiKeyOp redisMultiKeyOp = (RedisMultiKeyOp) redisOp;
            List<Pair<RedisKey,byte[]>> keyValues = redisMultiKeyOp.getAllKeyValues();
            for(Pair<RedisKey,byte[]> pair: keyValues){
                kafkaService.sendKafka(new GtidKeyItem(redisOp.getOpType().name(),gtidSeq[0],gtidSeq[1],pair.getKey().get(),null,dbId,replId));
            }
        }else if(redisOp instanceof RedisMultiSubKeyOp){
            RedisMultiSubKeyOp redisMultiSubKeyOp = (RedisMultiSubKeyOp) redisOp;
            RedisKey key = redisMultiSubKeyOp.getKey();
            List<RedisKey> subKeys = redisMultiSubKeyOp.getAllSubKeys();
            //包含子key的命令
            for(RedisKey subKey:subKeys) {
                kafkaService.sendKafka(new GtidKeyItem(redisOp.getOpType().name(),gtidSeq[0],gtidSeq[1],key.get(),subKey.get(),dbId,replId));
            }
        }else if(redisOp instanceof RedisSingleKeyOp) {
            RedisSingleKeyOp  redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
            kafkaService.sendKafka(new GtidKeyItem(redisOp.getOpType().name(),gtidSeq[0],gtidSeq[1],redisSingleKeyOp.getKey().get(),null,dbId,replId));
        }
    }

    public  void writeCK(String gtid,String dbId,List<RedisOp> redisOpList){
        for(RedisOp redisOp:redisOpList) {
            writeCK(gtid, dbId,redisOp);
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
}
