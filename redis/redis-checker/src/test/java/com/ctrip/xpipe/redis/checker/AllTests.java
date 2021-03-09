package com.ctrip.xpipe.redis.checker;

/**
 * @author lishanglin
 * date 2021/3/14
 */

import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        SentinelHelloTest.class,
        DefaultSiteReliabilityCheckerTest.class,
        HealthStatusTest.class,
        OuterClientServiceProcessorTest.class,
        SentinelHelloCheckActionFactoryTest.class,
        SentinelHelloCheckActionTest.class,
        RouteHealthEventProcessorTest.class,
        SentinelHelloActionDowngradeTest.class,
        SentinelLeakyBucketTest.class,
        CurrentDcDelayPingActionCollectorTest.class,

})
public class AllTests {
}
