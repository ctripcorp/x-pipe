package com.ctrip.xpipe.redis.integratedtest.stability;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.IpUtils;
import com.dianping.cat.Cat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *         <p>
 *         Oct 9, 2016
 */
public class XPipeStabilityTest {

    @JsonIgnore
    private Logger logger = LoggerFactory.getLogger(XPipeStabilityTest.class);

    public int runDays = Integer.parseInt(System.getProperty("run-days", "365"));
    public long MAX_MEMORY = Long.parseLong(System.getProperty("max-memory", String.valueOf(3L * (1 << 30))));

    private long maxKeys;

    public int TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("timeout", "10"));
    private int producerThreadNum = Integer.parseInt(System.getProperty("thread", "1"));
    private int producerIntervalMicro = Integer.parseInt(System.getProperty("producer-interval-micro", "1000"));

    private int msgSize = Integer.parseInt(System.getProperty("msg-size", "1000"));

    private String masterAddress = System.getProperty("master", "127.0.0.1:6379");
    private String slaveAddress = System.getProperty("slave", "127.0.0.1:6479");

    private TestMode testMode = null;


    @Before
    public void setUp() {

        maxKeys = MAX_MEMORY / msgSize;
        logger.info("config:{}", new JsonCodec(false, true).encode(this));

        Thread.setDefaultUncaughtExceptionHandler(new XPipeStabilityTestExceptionHandler() {
            @Override
            protected void doRestart() {
                logger.info("[{}][No-Restart]", Thread.currentThread());
            }
        });

    }

    @Test
    public void statbilityTest() throws Exception {

        String mode = System.getProperty("mode", "all");
        Cat.logEvent("statbilityTest", "begin");

        HostPort master = new HostPort(IpUtils.parseSingle(masterAddress));
        List<HostPort> slaves = new LinkedList<>();
        IpUtils.parse(slaveAddress).forEach(address -> slaves.add(new HostPort(address)));

        if (mode.equalsIgnoreCase("all")) {
            testMode = new AllKeyMode(
                    master,
                    slaves,
                    producerThreadNum,
                    producerIntervalMicro,
                    msgSize,
                    maxKeys
            );
        } else {
            testMode = new PubSubMode(
                    master,
                    slaves,
                    producerThreadNum,
                    producerIntervalMicro,
                    msgSize,
                    maxKeys
            );
        }

        logger.info("{}", testMode);
        testMode.test();
        TimeUnit.DAYS.sleep(runDays);
    }

    @After
    public void after() throws IOException {
        testMode.close();
    }

    public static abstract class XPipeStabilityTestExceptionHandler implements UncaughtExceptionHandler {
        private Logger logger = LoggerFactory.getLogger(getClass());

        @Override
        public void uncaughtException(Thread t, Throwable e) {

            if (e instanceof InterruptedException) {
                logger.error("[XPipeStabilityTestExceptionHandler][InterruptedException][{}]", Thread.currentThread(),
                        e);
            } else {
                logger.error("[XPipeStabilityTestExceptionHandler][UncaughtException][{}]", Thread.currentThread(), e);
                logger.info("[XPipeStabilityTestExceptionHandler][UncaughtException][{}]Try with doRestart",
                        Thread.currentThread());
                doRestart();
            }
        }

        protected abstract void doRestart();
    }
}
