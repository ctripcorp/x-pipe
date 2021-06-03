package com.ctrip.xpipe.redis.integratedtest.console;

import com.ctrip.xpipe.redis.integratedtest.console.dr.DRTest;
import com.ctrip.xpipe.redis.integratedtest.console.leader.ConsoleLeaderTest;
import com.ctrip.xpipe.redis.integratedtest.console.sentinel.SentinelMonitorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author lishanglin
 * date 2021/6/3
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        DRTest.class,
        ConsoleLeaderTest.class,
        SentinelMonitorTest.class
})
public class ConsoleAllTests {
}
