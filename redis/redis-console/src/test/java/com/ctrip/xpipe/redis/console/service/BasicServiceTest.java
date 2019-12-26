package com.ctrip.xpipe.redis.console.service;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	DcServiceTest.class,
	ClusterServiceTest.class,
	ShardServiceTest.class,
	KeeperContainerServiceTest.class,
	SetinelServiceTest.class
})
public class BasicServiceTest {	
}
