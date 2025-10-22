package com.ctrip.xpipe.redis.core.store.ck;

import com.ctrip.framework.ckafka.client.client.CKafkaClientBuilder;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.redis.operation.*;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.base.Throwables;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class CKStore {
    private static final Logger logger = LoggerFactory.getLogger(CKStore.class);

    private static final String TOPIC = "fx.cat.log.bbz-fx-xpipe-gtid";
    private static final String TOPIC_1 = "bbz.fx.xpipe.ck.gtid";
    private static final String ACL_USER = "kMTApwMDMzNzAws";
    private static final String CUSTOM_CLIENT_ID = "bbzfxxpipeckgtid";


    private  Producer<String, Object> producer;

    private  final Disruptor<MessageEvent> disruptor;

    private  final RingBuffer<MessageEvent> ringBuffer;

    private final Schema schema;

    private final long replId;

    private MetricProxy proxy = ServicesUtil.getMetricProxy();

    private static final String XPIPE_CK_KAFKA = "xpipe.ck.kafka";
    private static final String CK_BLOCK = "ck.block";

    private static final String schemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"GtidKeyItem\",\n" +
            "  \"namespace\": \"com.ctrip.xpipe.redis.core.store.ck\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"uuid\", \"type\": \"string\"},\n" +
            "    {\"name\": \"cmd\", \"type\": \"string\"},\n" +
            "    {\"name\": \"address\", \"type\": \"string\"},\n" +
            "    {\"name\": \"seq\", \"type\": \"string\"},\n" +
            "    {\"name\": \"key\", \"type\": {\"type\": \"array\", \"items\": [\"null\",\"int\"]}, \"default\": []},\n" +
            "    {\"name\": \"subkey\", \"type\": {\"type\": \"array\", \"items\": [\"null\",\"int\"]}, \"default\": []},\n" +
            "    {\"name\": \"dbid\", \"type\": \"string\"},\n" +
            "    {\"name\": \"timestamp\", \"type\": [\"null\",\"long\"], \"default\": null},\n" +
            "    {\"name\": \"shardid\", \"type\": \"int\"}\n" +
            "  ]\n" +
            "}";

    public CKStore(ReplId replId, RedisOpParser redisOpParser){
        this.replId = replId != null ? replId.id() : -1;

        schema = new Schema.Parser().parse(schemaJson);

        // 自定义配置，按需配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"268435456");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"50");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,"0");
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"4000000");
        properties.put(ProducerConfig.RETRIES_CONFIG,"2");
        properties.put(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG,"100");
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"50");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,ACL_USER+"-"+CUSTOM_CLIENT_ID);

        // 默认key为String序列化
        // 默认为value为HermesJson序列化
        try {
            if(Foundation.server().getEnv() != Env.LOCAL){
                producer = CKafkaClientBuilder // producer单例需要用户自己维护
                        .newProducerBuilder()
                        .hermesAvroSerializer()
                        .customProperties(properties) // 从此传入自定义配置，无则不需要关心
                        .topic(TOPIC_1) // 要发送的topic
                        .aclUser(ACL_USER) // acl token如有则替换填入此处，无则忽略
                        .build();
            }

            MessageEventFactory factory = new MessageEventFactory();
            int ringBufferSize = 1024 * 1024; // 1M个槽位

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

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendPayloads(List<Object[]> payloads) {
        long sequence = -1;  // 非阻塞获取序列号
        try {
            sequence = ringBuffer.tryNext();
            MessageEvent event = ringBuffer.get(sequence);
            event.setPayloads(payloads);
        } catch (Exception e){
            reportHickwall(CK_BLOCK);
        } finally {
            if(sequence != -1) {
                ringBuffer.publish(sequence);
            }
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
                sendKafka(redisOp.getOpType().name(),gtidSeq[0],gtidSeq[1],pair.getKey().get(),null,dbId,replId);
            }
        }else if(redisOp instanceof RedisMultiSubKeyOp){
            RedisMultiSubKeyOp redisMultiSubKeyOp = (RedisMultiSubKeyOp) redisOp;
            RedisKey key = redisMultiSubKeyOp.getKey();
            List<RedisKey> subKeys = redisMultiSubKeyOp.getAllSubKeys();
            //包含子key的命令
            for(RedisKey subKey:subKeys) {
                sendKafka(redisOp.getOpType().name(),gtidSeq[0],gtidSeq[1],key.get(),subKey.get(),dbId,replId);
            }
        }else if(redisOp instanceof RedisSingleKeyOp) {
            RedisSingleKeyOp  redisSingleKeyOp = (RedisSingleKeyOp) redisOp;
            sendKafka(redisOp.getOpType().name(),gtidSeq[0],gtidSeq[1],redisSingleKeyOp.getKey().get(),null,dbId,replId);
        }
    }

    public  void writeCK(String gtid,String dbId,List<RedisOp> redisOpList){
        for(RedisOp redisOp:redisOpList) {
            writeCK(gtid, dbId,redisOp);
        }
    }

    private GenericRecord genericRecord(String cmd,String uuid,String seq,byte[] key,byte[] subKey,String dbId,long shardId){
        GenericRecord record = new GenericData.Record(schema);
        record.put("cmd",cmd);
        record.put("uuid",uuid);
        record.put("address","");
        record.put("seq",seq);
        record.put("key", generateAvroArray("key",key));
        record.put("subkey",generateAvroArray("subkey",subKey));
        record.put("dbid",dbId);
        record.put("timestamp",System.currentTimeMillis()/1000);
        record.put("shardid",shardId);
        return record;
    }

    private GenericData.Array<Integer> generateAvroArray(String arrSchemaName,byte[] key){
        if(key == null) return new GenericData.Array<>(0,schema.getField(arrSchemaName).schema());
        Integer[] arr = new Integer[key.length];
        for(int k = 0;k<key.length;k++){
            arr[k] = (int) key[k];
        }

        GenericData.Array<Integer> keyArray = new GenericData.Array<>(
                schema.getField(arrSchemaName).schema(),
                Arrays.asList(arr)
        );
        return keyArray;
    }

    private void reportHickwall(String type) {
        MetricData data = new MetricData(type);
        data.setValue(1);
        data.setTimestampMilli(System.currentTimeMillis());
        try {
            proxy.writeBinMultiDataPoint(data);
        } catch (MetricProxyException e) {
            logger.warn("[xpipe][ck.reportHickwall]",e);
        }
    }

    private void sendKafka(String cmd,String uuid,String seq,byte[] key,byte[] subKey,String dbId,long shardId){
        try{
            producer.send(new ProducerRecord<>(TOPIC_1,genericRecord(cmd,uuid,seq,key,subKey,dbId,replId)));
        }catch (Exception e){
            EventMonitor.DEFAULT.logEvent(XPIPE_CK_KAFKA,e.getClass().getName(),convertProducerMetrics(producer.metrics(),e));
        }
    }

    private Map<String,String> convertProducerMetrics(Map<MetricName, ? extends Metric> metrics,Exception e){
        Map<String,String> metric = new HashMap<>(128);
        Throwable t = Throwables.getRootCause(e);
        metric.put("exception",t.getMessage());
        for(Map.Entry<MetricName,? extends Metric> entry:metrics.entrySet()){
            metric.put(entry.getKey().name(),String.valueOf(entry.getValue().metricValue()));
        }
        return metric;
    }

}
