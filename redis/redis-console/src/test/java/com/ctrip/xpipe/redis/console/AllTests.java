package com.ctrip.xpipe.redis.console;


import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.console.dal.ConcurrentDalTransactionTest;
import com.ctrip.xpipe.redis.console.dal.DalTransactionManagerTest;
import com.ctrip.xpipe.redis.console.service.BasicServiceTest;
import com.ctrip.xpipe.redis.console.service.MetaServiceTest;
import com.ctrip.xpipe.redis.console.service.RedisMetaServiceTest;
import com.ctrip.xpipe.redis.console.service.notifier.ClusterMetaModifiedNotifierTest;
import simpletest.SetOperationUtilTest;

import org.junit.runner.RunWith;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@RunWith(Suite.class)
@SuiteClasses({
	ConcurrentDalTransactionTest.class,
	DalTransactionManagerTest.class,
	ClusterMetaModifiedNotifierTest.class,
	BasicServiceTest.class,
	MetaServiceTest.class,
	RedisMetaServiceTest.class,
	SetOperationUtilTest.class
})
public class AllTests {

}
