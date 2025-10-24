package com.ctrip.xpipe.service.kafka;

import com.ctrip.framework.ckafka.client.client.CKafkaClientBuilder;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.google.common.base.Throwables;
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

public class CtripKafkaService implements KafkaService {
    private static final Logger logger = LoggerFactory.getLogger(CtripKafkaService.class);

    private static final String TOPIC_TEST = "fx.cat.log.bbz-fx-xpipe-gtid";
    private static final String TOPIC = "bbz.fx.xpipe.ck.gtid";
    private static final String ACL_USER = "kMTApwMDMzNzAws";
    private static final String CUSTOM_CLIENT_ID = "bbzfxxpipeckgtid";


    private  Producer<String, Object> producer;

    private final Schema schema;

    private static final String XPIPE_CK_KAFKA = "xpipe.ck.kafka";
    private static final String CK_BLOCK = "ck.block";

    private static final String schemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"GtidKeyItem\",\n" +
            "  \"namespace\": \"com.ctrip.xpipe.api.kafka\",\n" +
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

    public CtripKafkaService(){
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
                        .topic(TOPIC) // 要发送的topic
                        .aclUser(ACL_USER) // acl token如有则替换填入此处，无则忽略
                        .build();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private GenericRecord genericRecord(GtidKeyItem gtidKeyItem){
        GenericRecord record = new GenericData.Record(schema);
        record.put("cmd",gtidKeyItem.getCmd());
        record.put("uuid",gtidKeyItem.getUuid());
        record.put("address","");
        record.put("seq",gtidKeyItem.getSeq());
        record.put("key", generateAvroArray("key",gtidKeyItem.getKey()));
        record.put("subkey",generateAvroArray("subkey",gtidKeyItem.getSubkey()));
        record.put("dbid",gtidKeyItem.getDbid());
        record.put("timestamp",System.currentTimeMillis()/1000);
        record.put("shardid",gtidKeyItem.getShardid());
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

    @Override
    public void sendKafka(GtidKeyItem gtidKeyItem){
        try{
            producer.send(new ProducerRecord<>(TOPIC,genericRecord(gtidKeyItem)));
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

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
