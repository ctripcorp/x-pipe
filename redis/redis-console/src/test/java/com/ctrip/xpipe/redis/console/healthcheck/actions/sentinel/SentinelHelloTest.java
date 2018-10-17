package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public class SentinelHelloTest extends AbstractConsoleTest{

    @Test
    public void testFormat(){

        SentinelHello sentinelHello = SentinelHello.fromString("10.15.95.133,33322," +
                "642d5452b3ffd243fdfc31a2ccf5b0b5963c161f,1942," +
                "FlightIntlGDSCacheGroup1," +
                "10.15.94.178,6379,0");

        Assert.assertEquals(new HostPort("10.15.95.133", 33322), sentinelHello.getSentinelAddr());
        Assert.assertEquals(new HostPort("10.15.94.178", 6379), sentinelHello.getMasterAddr());
        Assert.assertEquals("FlightIntlGDSCacheGroup1", sentinelHello.getMonitorName());
    }

    @Test
    public void testEquals(){


        SentinelHello sentinelHello1 = SentinelHello.fromString("10.15.95.133,33322," +
                "642d5452b3ffd243fdfc31a2ccf5b0b5963c161f,1942," +
                "FlightIntlGDSCacheGroup1," +
                "10.15.94.178,6379,0");

        SentinelHello sentinelHello2 = SentinelHello.fromString("10.15.95.133,33322," +
                "642d5452b3ffd243fdfc31a2ccf5b0b5963c161f_XXX,1942abc," +
                "FlightIntlGDSCacheGroup1," +
                "10.15.94.178,6379,0");

        Assert.assertEquals(sentinelHello1, sentinelHello2);
    }

    }
