package com.ctrip.xpipe.service.kafka;

import com.ctrip.framework.ckafka.client.client.CKafkaClientBuilder;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.GtidTimestampItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.google.common.base.Throwables;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class CtripKafkaService implements KafkaService {

    private static final Logger logger = LoggerFactory.getLogger(CtripKafkaService.class);

    private static final String KEY_TOPIC = "bbz.fx.xpipe.ck.gtid";

    private static final String TIMESTAMP_TOPIC = "bbz.fx.xpipe.ck.gtid.timestamp";

    private static final String ACL_USER = "kMTApwMDMzNzAws";

    private static final String ACL_PRO_USER = "kMTApwMDA0Mzc2s";

    private static final String CUSTOM_CLIENT_ID = "bbzfxxpipeckgtid";

    private static final String TIMESTAMP_CLIENT_ID = "bbzfxxpipeckgtidtimestamp";

    private Producer<String,Object>[] producerPool;

    private volatile Producer<String,Object> timestampProducer;

    // 时间表每个 keeper 每分钟只有少量消息，缓冲不需要与 key producer 一样大
    private static final long TIMESTAMP_BUFFER_MEMORY = 4L * 1024 * 1024;

    private final int PRODUCER_POOL_SIZE = 16;

    private final int PRODUCER_POOL_MASK = PRODUCER_POOL_SIZE - 1;

    private static final String XPIPE_CK_KAFKA = "xpipe.ck.kafka";

    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean initSuccess = new AtomicBoolean(false);
    private AtomicBoolean timestampInitSuccess = new AtomicBoolean(false);

    public CtripKafkaService(){
        startProducer();
    }

    @Override
    public void startProducer(){
        if(started.compareAndSet(false,true)){
            Producer<String,Object>[] pool = new Producer[PRODUCER_POOL_SIZE];
            synchronized (this) {
                producerPool = pool;
            }
            long bufferMemory = Runtime.getRuntime().maxMemory() / 16 / PRODUCER_POOL_SIZE;
            new Thread(()->{
                String result = TransactionMonitor.DEFAULT.logTransactionSwallowException("KafkaProducer","initialize",()->{
                    long start = System.currentTimeMillis();
                    for (int i = 0; i < PRODUCER_POOL_SIZE; i++) {
                        pool[i] = createKafkaProducer(bufferMemory, KEY_TOPIC, CUSTOM_CLIENT_ID);
                    }
                    logger.info("[startProducer] elapse {}",System.currentTimeMillis()-start);
                    return "OK";
                });
                if(result != null){
                    initSuccess.set(true);
                }
                // 时间表 producer 独立初始化：失败不影响 key 发送，也不影响 CK_INIT 指标
                initTimestampProducer(pool, Math.min(bufferMemory, TIMESTAMP_BUFFER_MEMORY));
            },"kafka-producer-init").start();

        }
    }

    private void initTimestampProducer(Producer<String,Object>[] pool, long bufferMemory) {
        Producer<String, Object> producer;
        try {
            producer = createKafkaProducer(bufferMemory, TIMESTAMP_TOPIC, TIMESTAMP_CLIENT_ID);
        } catch (Throwable th) {
            logger.warn("[initTimestampProducer] fail", th);
            EventMonitor.DEFAULT.logEvent(XPIPE_CK_KAFKA, "TimestampProducer-InitFail");
            return;
        }
        if (producer == null) {
            // LOCAL env: createKafkaProducer returns null by design, treat as initialized
            timestampInitSuccess.set(true);
            return;
        }
        synchronized (this) {
            // 初始化期间可能已被 forceStopProducer 停止，或已重新启动生成了新的 pool
            if (started.get() && producerPool == pool) {
                timestampProducer = producer;
                timestampInitSuccess.set(true);
                return;
            }
        }
        producer.close(Duration.ZERO);
    }

    @Override
    public void forceStopProducer(){
        logger.info("[forceStopProducer]");
        if(started.compareAndSet(true,false)) {
            for (int i = 0; i < PRODUCER_POOL_SIZE; i++) {
                Producer<String, Object> producer = producerPool[i];
                if (producer != null) {
                    producer.close(Duration.ZERO);
                }
            }
            Producer<String, Object> currentTimestampProducer;
            synchronized (this) {
                currentTimestampProducer = timestampProducer;
                timestampProducer = null;
                timestampInitSuccess.set(false);
                producerPool = null;
            }
            if (currentTimestampProducer != null) {
                currentTimestampProducer.close(Duration.ZERO);
            }
        }
    }

    @Override
    public boolean initSuccess() {
        return initSuccess.get();
    }

    @Override
    public boolean timestampInitSuccess() {
        return timestampInitSuccess.get();
    }

    private Producer<String,Object> createKafkaProducer(long bufferMemory, String topic, String clientId) {
        Producer<String, Object> producer = null;
        // 自定义配置，按需配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "1048576");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory + "");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "4000000");
        properties.put(ProducerConfig.RETRIES_CONFIG, "2");
        properties.put(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG, "100");
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "50");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, ACL_USER + "-" + clientId);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomHermesSerializer.class.getCanonicalName());

        // 默认key为String序列化
        // 默认为value为HermesJson序列化
        try {
            if (Foundation.server().getEnv() != Env.LOCAL) {
                if (Foundation.server().getEnv() == Env.PRO) {
                    producer = CKafkaClientBuilder // producer单例需要用户自己维护
                            .newProducerBuilder()
//                            .hermesAvroSerializer()
                            .customProperties(properties) // 从此传入自定义配置，无则不需要关心
                            .topic(topic) // 要发送的topic
                            .aclUser(ACL_PRO_USER) // acl token如有则替换填入此处，无则忽略
                            .build();
                } else {
                    producer = CKafkaClientBuilder // producer单例需要用户自己维护
                            .newProducerBuilder()
//                            .hermesAvroSerializer()
                            .customProperties(properties) // 从此传入自定义配置，无则不需要关心
                            .topic(topic) // 要发送的topic
                            .aclUser(ACL_USER) // acl token如有则替换填入此处，无则忽略
                            .build();
                }

                try {
                    long start = System.currentTimeMillis();
                    producer.partitionsFor(topic);
                    logger.info("[partitionFor] elapse {}",System.currentTimeMillis()-start);
                } catch (Throwable t) {
                    logger.warn("init partition wait meta ready", t);
                }

                if (KEY_TOPIC.equals(topic)) {
                    // preheat the existing key topic without writing a different schema to the timestamp topic
                    long start = System.currentTimeMillis();
                    producer.send(new ProducerRecord<>(topic, GtidKeyItem.buildGtidKeyItem("test","uuid","0",
                                    "testk".getBytes(),"testv".getBytes(),"0",0,"localhost")));
                    logger.info("[sendKafka] elapse {}",System.currentTimeMillis()-start);
                }
            }
            return producer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean sendKafka(GtidKeyItem gtidKeyItem){
        long threadId =  Thread.currentThread().threadId();
        int index = Long.hashCode(threadId) & PRODUCER_POOL_MASK;
        Producer<String,Object> producer = producerPool[index];
        try{
            if(producer == null){
                EventMonitor.DEFAULT.logEvent(XPIPE_CK_KAFKA,"KafkaProducer-NotReady");
                return false;
            }
            producer.send(new ProducerRecord<>(KEY_TOPIC,gtidKeyItem));
            return true;
        }catch (Exception e){
            EventMonitor.DEFAULT.logEvent(XPIPE_CK_KAFKA,e.getClass().getName(),convertProducerMetrics(producer.metrics(),e));
            return false;
        }
    }

    @Override
    public boolean sendTimestampKafka(GtidTimestampItem gtidTimestampItem) {
        Producer<String, Object> producer = timestampProducer;
        if (producer == null) {
            EventMonitor.DEFAULT.logEvent(XPIPE_CK_KAFKA, "TimestampProducer-NotReady");
            return false;
        }
        try {
            producer.send(new ProducerRecord<>(TIMESTAMP_TOPIC, gtidTimestampItem));
            return true;
        } catch (Exception e) {
            EventMonitor.DEFAULT.logEvent(XPIPE_CK_KAFKA, "Timestamp-" + e.getClass().getName(),
                    convertProducerMetrics(producer.metrics(), e));
            return false;
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
