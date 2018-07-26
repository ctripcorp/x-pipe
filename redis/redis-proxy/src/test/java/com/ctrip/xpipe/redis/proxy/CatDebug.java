package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Jul 25, 2018
 */
public class CatDebug {
    @Test
    public void testCatDisable() throws InterruptedException {
        while (!Thread.interrupted()) {
            EventMonitor.DEFAULT.logEvent("test-cat", "test-cat");
            Thread.sleep(1000);
        }
    }
}
