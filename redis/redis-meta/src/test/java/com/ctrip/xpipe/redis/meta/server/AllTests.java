package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskExecutorTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskTriggerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultClusterServersTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.PeerMasterMetaServerStateChangeHandlerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.CurrentMasterChooseCommandTest;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.PeerMasterChooseCommandTest;
import com.ctrip.xpipe.redis.meta.server.crdt.master.impl.*;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.impl.DefaultPeerMasterStateAdjusterTest;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.impl.DefaultPeerMasterStateManagerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.impl.PeerMasterAdjustActionTest;
import com.ctrip.xpipe.redis.meta.server.crdt.replication.impl.PeerMasterAdjustJobFactoryTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.DefaultChangePrimaryDcActionTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.ClusterShardCachedNewMasterChooserTest;
import com.ctrip.xpipe.redis.meta.server.dchange.impl.*;
import com.ctrip.xpipe.redis.meta.server.impl.DefaultMetaServerRefreshPeerMasterTest;
import com.ctrip.xpipe.redis.meta.server.impl.DefaultMetaServersTest;
import com.ctrip.xpipe.redis.meta.server.job.ApplierStateChangeJobTest;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJobTest;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJobTest;
import com.ctrip.xpipe.redis.meta.server.job.PeerMasterAdjustJobTest;
import com.ctrip.xpipe.redis.meta.server.keeper.DefaultKeeperStateChangeHandlerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.DefaultApplierStateChangeHandlerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl.DefaultApplierMasterChooserManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.container.DefaultApplierContainerServiceTest;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.elect.DefaultApplierElectorManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.manager.DefaultApplierManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.manager.DefaultApplierStateControllerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.container.DefaultKeeperContainerServiceFactoryTest;
import com.ctrip.xpipe.redis.meta.server.keeper.container.DefaultKeeperContainerServiceTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.DefaultKeeperActiveElectAlgorithmManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.DefaultKeeperElectorManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.MultiPathKeeperElectorManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.UserDefinedPriorityKeeperActiveElectAlgorithmTest;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.*;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DefaultKeeperManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DefaultKeeperStateControllerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DeleteKeeperCommandTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaTest;
import com.ctrip.xpipe.redis.meta.server.meta.MetaJacksonTest;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultCurrentMetaManagerTest;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCacheRefreshTest;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCacheTest;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfoEditorTest;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfoTest;
import com.ctrip.xpipe.redis.meta.server.service.console.ConsoleServiceImplTest;
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
	DefaultDcMetaCacheTest.class,
	MetaJacksonTest.class,
	ArrangeTaskTriggerTest.class,
	ArrangeTaskExecutorTest.class,
	//DefaultClusterArrangerTest.class,
	DefaultClusterServersTest.class,
	//ClusterServerShardingTest.class,
	//ClusterServersApiTest.class,
	DefaultMetaServersTest.class,
	DefaultCurrentClusterServerTest.class,
	DefaultKeeperContainerServiceFactoryTest.class,
	ForwardInfoEditorTest.class,
	DefaultCurrentMetaManagerTest.class,
	ForwardInfoTest.class,
	AtLeastOneCheckerTest.class,
	CurrentMetaTest.class, 
	UserDefinedPriorityKeeperActiveElectAlgorithmTest.class,
	DefaultKeeperActiveElectAlgorithmManagerTest.class,
	DefaultKeeperElectorManagerTest.class,
	DefaultApplierElectorManagerTest.class,
	MultiPathKeeperElectorManagerTest.class,
	//AddKeeperCommandTest.class,
	DeleteKeeperCommandTest.class,
	BackupDcKeeperMasterChooserAlgorithmTest.class,
	PrimaryDcKeeperMasterChooserAlgorithmTest.class,
	HeteroDownStreamDcKeeperMasterChooserAlgorithmTest.class,
	DefaultDcKeeperMasterChooserTest.class,
	DefaultApplierMasterChooserManagerTest.class,
	FirstNewMasterChooserTest.class,
	DefaultOffsetwaiterTest.class,
	DefaultSlaveOfJobTest.class,
	DefaultKeeperStateControllerTest.class,
	DefaultApplierStateControllerTest.class,
	KeeperStateChangeJobTest.class,
	ApplierStateChangeJobTest.class,
	DefaultKeeperStateChangeHandlerTest.class,
	DefaultApplierStateChangeHandlerTest.class,
	DeferredResponseTest.class,
	DefaultSentinelManagerTest.class,
	DefaultDcMetaCacheRefreshTest.class,
	DefaultChangePrimaryDcActionTest.class,
	DefaultKeeperManagerTest.class,
	DefaultApplierManagerTest.class,
	PeerMasterChooserManagerTest.class,
	MasterChooserTest.class,
	CurrentMasterChooseCommandTest.class,
	PeerMasterChooseCommandTest.class,
	DefaultPeerMasterChooseActionTest.class,
	MasterChooseCommandFactoryTest.class,
	PeerMasterAdjustJobTest.class,
	DefaultPeerMasterStateAdjusterTest.class,
	DefaultPeerMasterStateManagerTest.class,
	PeerMasterAdjustActionTest.class,
	PeerMasterAdjustJobFactoryTest.class,
	PeerMasterMetaServerStateChangeHandlerTest.class,
	DefaultMetaServerRefreshPeerMasterTest.class,
	AbstractClusterShardPeriodicTaskTest.class,
	DefaultMasterChooserManagerTest.class,
	ClusterShardCachedNewMasterChooserTest.class,
	BecomePrimaryActionTest.class,
	DefaultKeeperContainerServiceTest.class,
	DefaultApplierContainerServiceTest.class,
	ConsoleServiceImplTest.class,
	DefaultRedisGtidCollectorTest.class
})
public class AllTests {

}
