package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.service.impl.SentinelGroupServiceMockTest;
import com.ctrip.xpipe.redis.console.service.impl.SentinelGroupServiceTest;
import com.ctrip.xpipe.redis.console.service.impl.SentinelServiceImplTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        DcServiceTest.class,
        ClusterServiceTest.class,
        ShardServiceTest.class,
        KeeperContainerCheckerServiceTest.class,
        SentinelGroupServiceTest.class,
        SentinelGroupServiceMockTest.class,
        SentinelServiceImplTest.class
})
public class BasicServiceTest {
}
