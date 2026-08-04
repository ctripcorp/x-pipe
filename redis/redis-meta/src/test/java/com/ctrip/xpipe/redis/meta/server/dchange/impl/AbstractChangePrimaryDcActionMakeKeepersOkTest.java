package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.SentinelManager;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.AbstractChangePrimaryDcAction;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * D31: makeKeepersOk must pass KeeperRoleAssigner roles so non-slot TFS stays PREPARE.
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractChangePrimaryDcActionMakeKeepersOkTest extends AbstractMetaServerTest {

	@Mock
	private DcMetaCache dcMetaCache;

	@Mock
	private CurrentMetaManager currentMetaManager;

	@Mock
	private SentinelManager sentinelManager;

	private final List<String> callOrder = Collections.synchronizedList(new ArrayList<>());

	private TestChangePrimaryDcAction action;

	@Before
	public void beforeAbstractChangePrimaryDcActionMakeKeepersOkTest() throws Exception {
		when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
			KeeperMeta keeperMeta = invocation.getArgument(0);
			KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
			keeperContainerMeta.setId(keeperMeta.getKeeperContainerId());
			keeperContainerMeta.setDiskType(keeperMeta.getKeeperContainerId() >= 2L ? "tfs-1" : "DEFAULT");
			return keeperContainerMeta;
		});
		when(currentMetaManager.getClusterMeta(anyLong())).thenReturn(new ClusterMeta().setActiveDc(getDc()));
		when(currentMetaManager.getClusterRouteByDcId(anyString(), anyLong())).thenReturn(null);
		doNothing().when(currentMetaManager).setKeeperMaster(anyLong(), anyLong(), anyString(), anyInt());

		action = new TestChangePrimaryDcAction(getClusterDbId(), getShardDbId(), dcMetaCache, currentMetaManager,
				sentinelManager, new ExecutionLog(getTestName()), getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
	}

	@Test
	public void testMakeKeepersOkBmActiveTwoTfsSendsPrepare() throws Exception {
		callOrder.clear();
		KeeperMeta bm = keeper(8101, 1L, 1, true);
		KeeperMeta highTfs = keeper(8102, 2L, 5, false);
		KeeperMeta lowTfs = keeper(8103, 3L, 1, false);
		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(bm);
		keepers.add(highTfs);
		keepers.add(lowTfs);

		when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(keepers);
		when(currentMetaManager.getKeeperActive(getClusterDbId(), getShardDbId())).thenReturn(bm);

		startKeeperServer(bm.getPort());
		startKeeperServer(highTfs.getPort());
		startKeeperServer(lowTfs.getPort());

		Pair<String, Integer> newMaster = new Pair<>("127.0.0.1", randomPort());
		action.invokeMakeKeepersOk(getClusterDbId(), getShardDbId(), newMaster);

		verify(currentMetaManager).setKeeperMaster(eq(getClusterDbId()), eq(getShardDbId()),
				eq(newMaster.getKey()), eq(newMaster.getValue()));
		Assert.assertTrue(callOrder.contains(bm.getPort() + ":ACTIVE"));
		Assert.assertTrue(callOrder.contains(highTfs.getPort() + ":BACKUP"));
		Assert.assertTrue(callOrder.contains(lowTfs.getPort() + ":PREPARE"));
		Assert.assertFalse(callOrder.contains(lowTfs.getPort() + ":BACKUP"));
	}

	@Test
	public void testMakeKeepersOkPureBmStillBackup() throws Exception {
		callOrder.clear();
		KeeperMeta active = keeper(8111, 1L, 1, true);
		KeeperMeta backup = keeper(8112, 1L, 1, false);
		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(active);
		keepers.add(backup);

		when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(keepers);
		when(currentMetaManager.getKeeperActive(getClusterDbId(), getShardDbId())).thenReturn(active);

		startKeeperServer(active.getPort());
		startKeeperServer(backup.getPort());

		Pair<String, Integer> newMaster = new Pair<>("127.0.0.1", randomPort());
		action.invokeMakeKeepersOk(getClusterDbId(), getShardDbId(), newMaster);

		Assert.assertTrue(callOrder.contains(active.getPort() + ":ACTIVE"));
		Assert.assertTrue(callOrder.contains(backup.getPort() + ":BACKUP"));
		Assert.assertFalse(callOrder.contains(backup.getPort() + ":PREPARE"));
	}

	@Test
	public void testMakeKeepersOkWithoutActiveSkipsSetstate() throws Exception {
		callOrder.clear();
		KeeperMeta bm = keeper(8121, 1L, 1, false);
		KeeperMeta highTfs = keeper(8122, 2L, 5, false);
		KeeperMeta lowTfs = keeper(8123, 3L, 1, false);
		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(bm);
		keepers.add(highTfs);
		keepers.add(lowTfs);

		when(currentMetaManager.getSurviveKeepers(getClusterDbId(), getShardDbId())).thenReturn(keepers);
		when(currentMetaManager.getKeeperActive(getClusterDbId(), getShardDbId())).thenReturn(null);

		startKeeperServer(bm.getPort());
		startKeeperServer(highTfs.getPort());
		startKeeperServer(lowTfs.getPort());

		Pair<String, Integer> newMaster = new Pair<>("127.0.0.1", randomPort());
		action.invokeMakeKeepersOk(getClusterDbId(), getShardDbId(), newMaster);

		verify(currentMetaManager).setKeeperMaster(eq(getClusterDbId()), eq(getShardDbId()),
				eq(newMaster.getKey()), eq(newMaster.getValue()));
		Assert.assertTrue(callOrder.isEmpty());
		Assert.assertFalse(callOrder.contains(highTfs.getPort() + ":BACKUP"));
		Assert.assertFalse(callOrder.contains(lowTfs.getPort() + ":BACKUP"));
	}

	private void startKeeperServer(int port) throws Exception {
		startServer(port, new AbstractIoActionFactory() {
			@Override
			protected byte[] getToWrite(Object readResult) {
				String state = parseKeeperSetState((String) readResult);
				if (state != null) {
					callOrder.add(port + ":" + state);
				}
				return "+OK\r\n".getBytes();
			}
		});
	}

	private String parseKeeperSetState(String request) {
		if (request == null) {
			return null;
		}
		if (request.contains("setstate PREPARE")) {
			return "PREPARE";
		}
		if (request.contains("setstate ACTIVE")) {
			return "ACTIVE";
		}
		if (request.contains("setstate BACKUP")) {
			return "BACKUP";
		}
		return null;
	}

	private KeeperMeta keeper(int port, long keeperContainerId, Integer priority, boolean active) {
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("127.0.0.1");
		keeperMeta.setPort(port);
		keeperMeta.setKeeperContainerId(keeperContainerId);
		keeperMeta.setPriority(priority);
		keeperMeta.setActive(active);
		return keeperMeta;
	}

	private static class TestChangePrimaryDcAction extends AbstractChangePrimaryDcAction {

		TestChangePrimaryDcAction(Long clusterDbId, Long shardDbId, DcMetaCache dcMetaCache,
								  CurrentMetaManager currentMetaManager, SentinelManager sentinelManager,
								  ExecutionLog executionLog, XpipeNettyClientKeyedObjectPool keyedObjectPool,
								  ScheduledExecutorService scheduled, Executor executors) {
			super(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, sentinelManager, executionLog,
					keyedObjectPool, scheduled, executors);
			this.waitTimeoutSeconds = 4;
		}

		void invokeMakeKeepersOk(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster) {
			makeKeepersOk(clusterDbId, shardDbId, newMaster);
		}

		@Override
		protected PrimaryDcChangeMessage doChangePrimaryDc(Long clusterDbId, Long shardDbId, String newPrimaryDc,
														   MasterInfo masterInfo) {
			return null;
		}

		@Override
		protected void changeSentinel(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster) {
		}

		@Override
		protected void makeRedisesOk(Pair<String, Integer> newMaster, List<RedisMeta> slaves) {
		}

		@Override
		protected List<RedisMeta> getAllSlaves(Pair<String, Integer> newMaster, List<RedisMeta> shardRedises) {
			return Collections.emptyList();
		}

		@Override
		protected Pair<String, Integer> chooseNewMaster(Long clusterDbId, Long shardDbId) {
			return null;
		}
	}
}
