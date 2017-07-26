package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.monitor.DelayMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.monitor.DefaultDelayMonitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 25, 2017
 */
public class Log4jTest extends AbstractTest {

    private DelayMonitor delayMonitor;

    private int concurrentCount = 50;

    @Before
    public void beforeLog4jTest() throws Exception {
        delayMonitor = new DefaultDelayMonitor("logtest");
        delayMonitor.start();
        delayMonitor.setConsolePrint(true);
        scheduled = Executors.newScheduledThreadPool(concurrentCount);

    }

    @Test
    public void simpleLog(){
        logger.info("{}", getTestName());
    }

    @Test
    public void testLatnecy() throws IOException {

        String random = randomString(1024);
        int microMilli = 1000;

        for (int i = 0; i < concurrentCount; i++) {

            scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

                @Override
                protected void doRun() throws Exception {

                    long begin = System.currentTimeMillis();
                    logger.info(random);
                    delayMonitor.addData(begin);
                }
            }, 0, microMilli, TimeUnit.MICROSECONDS);
        }
        logger.info("{}", random);
        waitForAnyKeyToExit();

    }

    @After
    public void afterLog4jTest() throws Exception {
        delayMonitor.stop();
    }


}
