package com.ctrip.xpipe.redis.integratedtest.stability;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 13, 2018
 */
public abstract class AbstractTestMode implements TestMode {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public int QPS_COUNT_INTERVAL = Integer.parseInt(System.getProperty("qps-count-interval", "5"));
    public int TIME_TOO_LONG_TO_LOG_MILLI = Integer.parseInt(System.getProperty("time-long-log", "10"));
    public static final String testMessagePrefix = "test";

    protected MetricLog metricLog = MetricLog.create();


    private final int producerThreadNum;
    private final int producerIntervalMicro;
    private final long maxKeys;
    private ScheduledExecutorService producerThreadPool;
    protected ExecutorService consumerThreadPool;
    private ScheduledExecutorService qpsCheckThreadPool;
    protected JedisPool masterPool;
    protected Map<HostPort, JedisPool> slavePools = new ConcurrentHashMap<>();
    private ObjectPool<byte[]> dataPool;
    private AtomicLong globalCnt = new AtomicLong(0);
    private AtomicLong queryCnt = new AtomicLong(0);
    private Long historyQueryCnt = new Long(0);
    protected Map<HostPort, DelayManager> allDelays = new ConcurrentHashMap<>();
    private DelayManager beginSend;
    protected HostPort master;
    protected List<HostPort> slaves;

    public AbstractTestMode(HostPort master, List<HostPort> slaves, int producerThreadNum, int producerIntervalMicro, int msgSize, long maxKeys) {
        this.master = master;
        this.slaves = slaves;
        this.producerThreadNum = producerThreadNum;
        this.producerIntervalMicro = producerIntervalMicro;
        this.maxKeys = maxKeys;

        producerThreadPool = Executors.newScheduledThreadPool(producerThreadNum,
                XpipeThreadFactory.create("ProducerThreadPool"));
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(producerThreadNum * 3);
        dataPool = new GenericObjectPool<>(new BytesFactory(msgSize), config);
        consumerThreadPool = Executors.newFixedThreadPool(slaves.size() * 2 + 1, XpipeThreadFactory.create("ConsumerThreadPool"));
        qpsCheckThreadPool = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("QpsCheckThreadPool"));

        masterPool = getJedisPool(master.getHost(), master.getPort(), producerThreadNum * 2, producerThreadNum);
        beginSend = new DelayManager(qpsCheckThreadPool, "beginsend", master.toString(), TIME_TOO_LONG_TO_LOG_MILLI, false);
        slaves.forEach((slave) -> allDelays.put(slave, new DelayManager(qpsCheckThreadPool, "delay", String.format("%s.%d.%d.%d", slave.getHost(), slave.getPort(), producerThreadNum, producerIntervalMicro), TIME_TOO_LONG_TO_LOG_MILLI)));
        slaves.forEach((slave) -> slavePools.put(slave, getJedisPool(slave.getHost(), slave.getPort(), 2, 2)));

    }

    @Override
    public void test() throws Exception {

        logger.info("[test][start]");
        doTest();
    }

    protected void doTest() throws InterruptedException {

        startQpsCheckJob();
        startConsumerJob();
        startProducerJob();

    }

    protected void startConsumerJob() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(slavePools.size());

        slavePools.forEach((slaveHostport, slavePool) -> {

            consumerThreadPool.execute(new Runnable() {
                @SuppressWarnings("static-access")
                @Override
                public void run() {
                    Thread.currentThread().setDefaultUncaughtExceptionHandler(new XPipeStabilityTest.XPipeStabilityTestExceptionHandler() {
                        @Override
                        protected void doRestart() {
                            logger.info("[doRestart]Consumer Job");
                            try {
                                startConsumerJob();
                            } catch (InterruptedException e) {
                                logger.error("[doRestart]" + slaveHostport, e);
                            }
                        }
                    });

                    Jedis slave = null;
                    try {
                        slave = slavePool.getResource();
                        slave.psubscribe(new JedisPubSub() {
                            @Override
                            public void onPMessage(String pattern, String channel, String message) {
                                try {
                                    latch.countDown();
                                    if(isTestMessage(message)){
                                        logger.info("[onPMessage][testmessage]{}, {}", channel, message);
                                        return;
                                    }
                                    AbstractTestMode.this.onPMessage(slaveHostport, allDelays.get(slaveHostport), message);
                                } catch (Exception e) {
                                    logger.error("[onPMessage]", e);
                                }
                            }
                        }, getPattern());

                    } finally {
                        if (null != slave) {
                            slave.close();
                        }
                    }
                }
            });
        });

        sendSomeTestMessage();//try make sure consumer has started
        boolean await = latch.await(5, TimeUnit.SECONDS);
        logger.info("[startConsumerJob][wait]{}", await);
    }

    protected abstract void sendSomeTestMessage();

    protected abstract void onPMessage(HostPort slave, DelayManager delayManager, String message);

    protected abstract String getPattern();


    protected void startProducerJob() {

        for (int jobCnt = 0; jobCnt != producerThreadNum; ++jobCnt) {
            producerThreadPool.scheduleAtFixedRate(new ProducerThread(), 0,
                    producerIntervalMicro,
                    TimeUnit.MICROSECONDS);
        }
    }

    private class ProducerThread extends AbstractExceptionLogTask {

        private UnsignedLongByte key = new UnsignedLongByte();
        private UnsignedLongByte nanoTime = new UnsignedLongByte();
        private UnsignedLongByte currentMilli = new UnsignedLongByte();
        byte[] preBytes = new byte[50];

        @Override
        public void doRun() {
            Jedis master = null;
            byte[] value = null;

            try {
                master = masterPool.getResource();
                value = dataPool.borrowObject();
                long milli = System.currentTimeMillis();
                long nano = System.nanoTime();

                key.from(currentKey());
                nanoTime.from(nano);
                currentMilli.from(milli);


                int preIndex = nanoTime.put(preBytes);
                preBytes[preIndex++] = '-';
                preIndex += currentMilli.put(preBytes, preIndex);
                preBytes[preIndex++] = '-';

                buildValue(value, preBytes, preIndex);

                String pre = new String(preBytes, 0, preIndex);

                String strKey = key.toString();
                putRecord(strKey, pre);

                beginSend.delay(System.nanoTime() - nano);
                master.set(key.getBytes(), value);
                queryCnt.incrementAndGet();
            } catch (Exception e) {
                logger.error(String.format("[startProducerJob][run]InsertValue Exception : Key:%s", key), e);
                removeRecordsPutException(key.toString());
            } finally {
                if (value != null) {
                    try {
                        dataPool.returnObject(value);
                    } catch (Exception e) {
                        logger.error("[run]", e);
                    }
                }
                if (null != master) {
                    master.close();
                }
            }
        }
    }

    private long currentKey() {
        long current = globalCnt.getAndIncrement() % maxKeys;
        if (current == 0) {
            logger.info("[currentKey][back to zero]{}", current);

        }
        return current;
    }

    protected abstract void removeRecordsPutException(String key);

    protected abstract void putRecord(String strKey, String pre);


    public Pair<Long, Long> extractTimeFromValue(String value) {

        int indexNano = value.indexOf("-");
        int indexMilli = value.indexOf("-", indexNano + 1);
        if (indexMilli == -1) {
            indexMilli = value.length();
        }

        return Pair.of(Long.parseLong(value.substring(0, indexNano)),
                Long.parseLong(value.substring(indexNano + 1, indexMilli)));
    }

    protected String valueForPrint(String value) {

        if (value == null) {
            return null;
        }

        int end = Math.min(45, value.length());
        return value.substring(0, end);
    }

    private void buildValue(byte[] data, byte[] preBytes, int len) {

        for (int i = 0; i < len; i++) {
            data[i] = preBytes[i];
        }
    }


    private void startQpsCheckJob() {

        qpsCheckThreadPool.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {

                long qps = (queryCnt.get() - historyQueryCnt) / QPS_COUNT_INTERVAL;
                historyQueryCnt = queryCnt.get();
                metricLog.log("qps", String.format("%s.%d", master.getHost(), master.getPort()), qps);
                logger.info("[startQpsCheckJob][run]QPS: {}", qps);
                doPrintQpsInfo();
            }
        }, 1, QPS_COUNT_INTERVAL, TimeUnit.SECONDS);
    }

    protected boolean isTestMessage(String message) {
        return message != null && message.startsWith(testMessagePrefix);
    }

    protected abstract void doPrintQpsInfo();

    protected JedisPool getJedisPool(String ip, int port, int maxTotal, int maxIdle) {
        return getJedisPool(ip, port, maxTotal, maxIdle, 5000);
    }

    protected JedisPool getJedisPool(String ip, int port, int maxTotal, int maxIdle, int timeout) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);
        return new JedisPool(config, ip, port, timeout);
    }

    @Override
    public void close() throws IOException {
        logger.info("[tearDown]");
        producerThreadPool.shutdownNow();
        consumerThreadPool.shutdownNow();
        qpsCheckThreadPool.shutdownNow();
        masterPool.destroy();
        slavePools.forEach((slave, slavePool) -> slavePool.destroy());

        doClose();

    }

    protected abstract void doClose();
}
