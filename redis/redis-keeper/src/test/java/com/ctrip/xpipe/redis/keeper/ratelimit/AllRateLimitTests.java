package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.redis.keeper.impl.fakeredis.FakeRedisRdbDumpLong;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 * <p>
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
        FakeRedisRdbDumpLong.class,
        RateLimitTest.class,
})
public class AllRateLimitTests {

}
