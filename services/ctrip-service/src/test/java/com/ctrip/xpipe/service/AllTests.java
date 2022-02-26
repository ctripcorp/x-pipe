package com.ctrip.xpipe.service;

import com.ctrip.xpipe.service.beacon.BeaconServiceTest;
import com.ctrip.xpipe.service.client.redis.CRedisAsyncClientTest;
import com.ctrip.xpipe.service.fireman.XPipeFiremanDependencyTest;
import com.ctrip.xpipe.service.foundation.CtripFoundationServiceTest;
import com.ctrip.xpipe.service.metric.HickwallMetricTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        XPipeFiremanDependencyTest.class,
        BeaconServiceTest.class,
        CtripFoundationServiceTest.class,
        HickwallMetricTest.class,
        CRedisAsyncClientTest.class
})
public class AllTests {
}
