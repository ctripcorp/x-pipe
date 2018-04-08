package com.ctrip.xpipe.redis.integratedtest.stability;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 13, 2018
 */
public class PubSubMode extends AbstractTestMode {

    private int pubIntervalMilli = Integer.parseInt(System.getProperty("PubIntervalMilli", "10"));
    private String pubChannel = "xpipe_pub_sub_key";
    private ScheduledExecutorService scheduled;

    public PubSubMode(HostPort master, List<HostPort> slaves, int producerThreadNum, int producerIntervalMicro, int msgSize, long maxKeys) {
        super(master, slaves, producerThreadNum, producerIntervalMicro, msgSize, maxKeys);
        scheduled = Executors.newScheduledThreadPool(4);
    }

    @Override
    protected void sendSomeTestMessage() {
        masterPool.getResource().publish(pubChannel, testMessagePrefix + "-value");
    }

    @Override
    protected void onPMessage(HostPort slave, DelayManager delayManager, String message) {

        long currentNanos = System.nanoTime();
        long previous = Long.valueOf(message);

        long delay = currentNanos - previous;
        delayManager.delay(delay);
    }

    @Override
    protected String getPattern() {
        return pubChannel;
    }

    @Override
    protected void startProducerJob() {
        super.startProducerJob();

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {

                try (Jedis resource = masterPool.getResource()) {

                    long currentNanos = System.nanoTime();
                    String currentNanosStr = String.valueOf(currentNanos);
                    logger.debug("[publish]{}", currentNanosStr);
                    resource.publish(pubChannel, currentNanosStr);
                }
            }
        }, pubIntervalMilli, pubIntervalMilli, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void removeRecordsPutException(String key) {

    }

    @Override
    protected void putRecord(String strKey, String pre) {

    }

    @Override
    protected void doPrintQpsInfo() {

    }

    @Override
    protected void doClose() {

    }
}
