package com.ctrip.xpipe.redis.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.core.meta.MetaCloneTest;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManagerTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommandTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParserTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParserTest;
import com.ctrip.xpipe.redis.core.redis.DefaultRunIdGeneratorTest;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMetaTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	DefaultRunIdGeneratorTest.class,
	ArrayParserTest.class,
	BulkStringParserTest.class,
	ArrayParserTest.class,
	MetaCloneTest.class,
	DefaultXpipeMetaManagerTest.class,
	ReplicationStoreMetaTest.class,
	DcMetaComparatorTest.class,
	ClusterMetaComparatorTest.class,
	ShardMetaComparatorTest.class,
	RoleCommandTest.class
})
public class AllTests {

}
