package com.ctrip.xpipe.redis.console.simple;

import com.ctrip.xpipe.monitor.CatConfig;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         May 09, 2017
 */
public class CatTest extends AbstractConsoleTest{

    private int count = 1;

    @Before
    public void beforeCatTest(){
        System.setProperty(CatConfig.CAT_ENABLED_KEY, "true");
    }

    @Test
    public void testAlert() throws IOException {

        scheduled.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                CatEventMonitor.DEFAULT.logAlertEvent(String.valueOf(count++));
            }
        }, 0, 5, TimeUnit.SECONDS);


        waitForAnyKeyToExit();

    }
}
