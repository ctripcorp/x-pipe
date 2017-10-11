package com.ctrip.xpipe.redis.core;

import com.ctrip.xpipe.redis.core.meta.ClusterShardCounterTest;
import com.ctrip.xpipe.redis.core.meta.DcInfoTest;
import com.ctrip.xpipe.redis.core.meta.MetaCloneTest;
import com.ctrip.xpipe.redis.core.meta.QuorumConfigTest;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManagerTest;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICETest;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfoTest;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRoleTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.*;
import com.ctrip.xpipe.redis.core.redis.DefaultRunIdGeneratorTest;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMetaTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	QuorumConfigTest.class,
	DefaultRunIdGeneratorTest.class,
	ArrayParserTest.class,
	BulkStringParserTest.class,
	ArrayParserTest.class,
	RedisErrorParserTest.class,
	MetaCloneTest.class,
	ClusterShardCounterTest.class,
	DefaultXpipeMetaManagerTest.class,
	ReplicationStoreMetaTest.class,
	DcMetaComparatorTest.class,
	ClusterMetaComparatorTest.class,
	ShardMetaComparatorTest.class,
	RoleCommandTest.class,
	MasterRoleTest.class,
	MasterInfoTest.class,
	DcInfoTest.class,
	DefaultPsyncTest.class,
	META_SERVER_SERVICETest.class,
	BulkStringEofJudgerTest.class,
	BulkStringEofJuderManagerTest.class,
	RoleCommandTest.class,
	PingCommandTest.class,
	RedisCommandTest.class,
	DefaultSlaveOfCommandTest.class
})
public class AllTests {

}
