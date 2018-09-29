package com.ctrip.xpipe.redis.integratedtest.stability;

import com.ctrip.xpipe.endpoint.HostPort;
import org.apache.commons.lang3.tuple.Pair;
import redis.clients.jedis.JedisPool;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 13, 2018
 */
public class AllKeyMode extends AbstractTestMode {

    private boolean startValueCheck = Boolean.parseBoolean(System.getProperty("start-value-check", "true"));
    private ValueCheck valueCheck;
    private int valueCheckThreadNum = Integer.parseInt(System.getProperty("valueCheckThread", "8"));
    protected ConcurrentHashMap<String, String> records = new ConcurrentHashMap<>(20000);


    public AllKeyMode(HostPort master, List<HostPort> slaves, int producerThreadNum, int producerIntervalMicro, int msgSize, long maxKeys) {
        super(master, slaves, producerThreadNum, producerIntervalMicro, msgSize, maxKeys);
        valueCheckThreadNum = (producerThreadNum < valueCheckThreadNum) ? valueCheckThreadNum : producerThreadNum;
        logger.info("[ProducerThread]{} [ValueCheckThread]{}", producerThreadNum, valueCheckThreadNum);
    }

    @Override
    public void test() throws Exception {

        if (slaves.size() > 1) {
            throw new IllegalStateException("all key mode support only one slave!!!");
        }

        HostPort slave = slaves.get(0);
        if (startValueCheck) {
            logger.info("[test][addValueCheck]");
            JedisPool slavePool = getJedisPool(slave.getHost(), slave.getPort(), valueCheckThreadNum * 2, valueCheckThreadNum);
            valueCheck = new DefaultValueCheck(valueCheckThreadNum, slavePool, metricLog);
        } else {
            logger.info("[test][NullValueCheck]");
            valueCheck = new NullValueCheck();
        }
        valueCheck.start();
        super.test();
    }

    @Override
    protected void sendSomeTestMessage() {
        masterPool.getResource().set(testMessagePrefix + "-key", testMessagePrefix + "-value");
    }

    @Override
    protected String getPattern() {
        return "__key*__:*";
    }


    protected void putRecord(String key, String value) {

        String previous = records.put(key, value);
        if (previous != null) {
            Pair<Long, Long> date = extractTimeFromValue(value);
            String masterDesc = String.format("%s.%d", master.getHost(), master.getPort());
            metricLog.log("notice.lack", masterDesc, 1);
            logger.error("[putRecord][replace but old value still exists]{}, previous:{}, {}", key, valueForPrint(previous), new Date(date.getRight()));

        }
    }

    @Override
    protected void doPrintQpsInfo() {

        String masterDesc = String.format("%s.%d", master.getHost(), master.getPort());
        metricLog.log("map", masterDesc, records.size());
        metricLog.log("queue", masterDesc, valueCheck.queueSize());

        logger.info("[startQpsCheckJob][run]MapSize : {}", records.size());
        logger.info("[startQpsCheckJob][run]QueueSize : {}", valueCheck.queueSize());
    }


    @Override
    protected void removeRecordsPutException(String key) {
        records.remove(key);
    }

    @Override
    public void onPMessage(HostPort slave, DelayManager delayManager, String message) {

        String key = message;
        String value = records.get(key);
        if (null != value) {
            valueCheck.offer(Pair.of(key, value));
            records.remove(key);

            long current = System.nanoTime();
            long produceTime = Long.valueOf(value.substring(0, value.indexOf("-")));
            long delay = current - produceTime;
            delayManager.delay(delay);
        } else {
            logger.error("[XPipeStabilityTestJedisPubSub][Get Null From records]Key:{} Value:{}", key, value);
        }
    }

    @Override
    protected void doClose() {
        try {
            valueCheck.stop();
        } catch (Exception e) {
            logger.error("[value check error]", e);
        }
    }
}
