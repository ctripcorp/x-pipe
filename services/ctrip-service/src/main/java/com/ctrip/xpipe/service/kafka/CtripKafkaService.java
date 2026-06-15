package com.ctrip.xpipe.service.kafka;

import com.ctrip.framework.ckafka.client.client.CKafkaClientBuilder;
import com.ctrip.framework.foundation.Env;
import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.kafka.GtidKeyItem;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CtripKafkaService implements KafkaService {

    private static final Logger logger = LoggerFactory.getLogger(CtripKafkaService.class);

    private static final String TOPIC = "bbz.fx.xpipe.ck.gtid";

    private static final String ACL_USER = "kMTApwMDMzNzAws";

    private static final String ACL_PRO_USER = "kMTApwMDA0Mzc2s";

    private static final String CUSTOM_CLIENT_ID = "bbzfxxpipeckgtid";

    private Producer<String,Object>[] producerPool;

    private final int PRODUCER_POOL_SIZE = 16;

    private final int PRODUCER_POOL_MASK = PRODUCER_POOL_SIZE - 1;

    private static final String XPIPE_CK_KAFKA = "xpipe.ck.kafka";

    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean initSuccess = new AtomicBoolean(false);

    public CtripKafkaService(){
        startProducer();
    }

    @Override
    public void startProducer(){
        if(started.compareAndSet(false,true)){
            producerPool = new Producer[PRODUCER_POOL_SIZE];
            long bufferMemory = Runtime.getRuntime().maxMemory() / 16 / PRODUCER_POOL_SIZE;
            new Thread(()->{
                String result = TransactionMonitor.DEFAULT.logTransactionSwallowException("KafkaProducer","initialize",()->{
                    long start = System.currentTimeMillis();
                    for (int i = 0; i < PRODUCER_POOL_SIZE; i++) {
                        producerPool[i] = createKafkaProducer(bufferMemory);
                    }
                    logger.info("[startProducer] elapse {}",System.currentTimeMillis()-start);
                    return "OK";
                });
                if(result != null){
                    initSuccess.set(true);
                }
            },"kafka-producer-init").start();

            Runtime.getRuntime().addShutdownHook(new Thread(this::forceStopProducer));
        }
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
            producerPool = null;
        }
    }

    @Override
    public boolean initSuccess() {
        return initSuccess.get();
    }

    private Producer<String,Object> createKafkaProducer(long bufferMemory) {
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
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, ACL_USER + "-" + CUSTOM_CLIENT_ID);
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
                            .topic(TOPIC) // 要发送的topic
                            .aclUser(ACL_PRO_USER) // acl token如有则替换填入此处，无则忽略
                            .build();
                } else {
                    producer = CKafkaClientBuilder // producer单例需要用户自己维护
                            .newProducerBuilder()
//                            .hermesAvroSerializer()
                            .customProperties(properties) // 从此传入自定义配置，无则不需要关心
                            .topic(TOPIC) // 要发送的topic
                            .aclUser(ACL_USER) // acl token如有则替换填入此处，无则忽略
                            .build();
                }

                try {
                    long start = System.currentTimeMillis();
                    producer.partitionsFor(TOPIC);
                    logger.info("[partitionFor] elapse {}",System.currentTimeMillis()-start);
                } catch (Throwable t) {
                    logger.warn("init partition wait meta ready", t);
                }

                //preheat
                long start = System.currentTimeMillis();
                producer.send(new ProducerRecord<>(TOPIC, GtidKeyItem.buildGtidKeyItem("test","uuid","0",
                                "testk".getBytes(),"testv".getBytes(),"0",0,"localhost")));
                logger.info("[sendKafka] elapse {}",System.currentTimeMillis()-start);
            }
            return producer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendKafka(GtidKeyItem gtidKeyItem){
        long threadId =  Thread.currentThread().threadId();
        int index = Long.hashCode(threadId) & PRODUCER_POOL_MASK;
        Producer<String,Object> producer = producerPool[index];
        try{
            if(producer == null){
                EventMonitor.DEFAULT.logEvent(XPIPE_CK_KAFKA,"KafkaProducer-NotReady");
                return;
            }
            producer.send(new ProducerRecord<>(TOPIC,gtidKeyItem));
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
