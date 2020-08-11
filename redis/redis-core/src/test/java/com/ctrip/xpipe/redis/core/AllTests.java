package com.ctrip.xpipe.redis.core;

import com.ctrip.xpipe.redis.core.meta.ClusterShardCounterTest;
import com.ctrip.xpipe.redis.core.meta.DcInfoTest;
import com.ctrip.xpipe.redis.core.meta.MetaCloneTest;
import com.ctrip.xpipe.redis.core.meta.QuorumConfigTest;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparatorTest;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManagerTest;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICETest;
import com.ctrip.xpipe.redis.core.metaserver.impl.FastMetaServerConsoleServiceTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.*;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.CrdtPublishCommandTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.CrdtSubscribeCommandTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.TestAbstractSubscribeTest;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfoTest;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRoleTest;
import com.ctrip.xpipe.redis.core.protocal.protocal.*;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyMonitorCommandTest;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyPingCommandTest;
import com.ctrip.xpipe.redis.core.proxy.monitor.SessionTrafficResultTest;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelTrafficResultTest;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParserTest;
import com.ctrip.xpipe.redis.core.proxy.parser.TestForAbstractProxyProtocolParser;
import com.ctrip.xpipe.redis.core.proxy.parser.content.CompressParserTest;
import com.ctrip.xpipe.redis.core.proxy.parser.content.DefaultProxyContentParserTest;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocolTest;
import com.ctrip.xpipe.redis.core.redis.DefaultRunIdGeneratorTest;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMetaTest;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.redis.core.util.SentinelUtilTest;
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
	InfoResultExtractorTest.class,
	RoleCommandTest.class,
	KeeperCommandTest.class,
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
	DefaultSlaveOfCommandTest.class,
	ProxyMonitorCommandTest.class,
	ProxyPingCommandTest.class,
	CompressParserTest.class,
	DefaultProxyContentParserTest.class,
	DefaultProxyConnectProtocolTest.class,
	DefaultProxyConnectProtocolParserTest.class,
	TestForAbstractProxyProtocolParser.class,
	SessionTrafficResultTest.class,
	TunnelTrafficResultTest.class,
	DcRouteMetaComparatorTest.class,
	SentinelUtilTest.class,
	CrdtPublishCommandTest.class,
	CrdtSubscribeCommandTest.class,
	TestAbstractSubscribeTest.class,
	FastMetaServerConsoleServiceTest.class
})
public class AllTests {

}
