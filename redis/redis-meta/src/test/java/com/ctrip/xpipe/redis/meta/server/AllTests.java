package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.redis.meta.server.dchange.impl.AtLeastOneCheckerTest;
import com.ctrip.xpipe.redis.meta.server.meta.MetaJacksonTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerShardingTest;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServersApiTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskExecutorTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskTriggerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultClusterArrangerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServerTest;
import com.ctrip.xpipe.redis.meta.server.dchange.impl.FirstNewMasterChooserTest;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJobTest;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJobTest;
import com.ctrip.xpipe.redis.meta.server.keeper.DefaultKeeperStateChangeHandlerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.container.DefaultKeeperContainerServiceFactoryTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.DefaultKeeperActiveElectAlgorithmManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.DefaultKeeperElectorManagerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.UserDefinedPriorityKeeperActiveElectAlgorithmTest;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.BackupDcKeeperMasterChooserAlgorithmTest;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.DefaultDcKeeperMasterChooserTest;
import com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl.PrimaryDcKeeperMasterChooserAlgorithmTest;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.AddKeeperCommandTest;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DefaultKeeperStateControllerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DeleteKeeperCommandTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaTest;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultCurrentMetaManagerTest;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfoEditorTest;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfoTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultClusterServersTest;


/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	MetaJacksonTest.class,
	ArrangeTaskTriggerTest.class,
	ArrangeTaskExecutorTest.class,
	DefaultClusterArrangerTest.class,
	DefaultClusterServersTest.class,
	ClusterServerShardingTest.class,
	ClusterServersApiTest.class,
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
	AddKeeperCommandTest.class,
	DeleteKeeperCommandTest.class,
	BackupDcKeeperMasterChooserAlgorithmTest.class,
	PrimaryDcKeeperMasterChooserAlgorithmTest.class,
	DefaultDcKeeperMasterChooserTest.class,
	FirstNewMasterChooserTest.class,
	DefaultSlaveOfJobTest.class,
	DefaultKeeperStateControllerTest.class,
	KeeperStateChangeJobTest.class,
	DefaultKeeperStateChangeHandlerTest.class
})
public class AllTests {

}
