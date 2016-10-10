package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.metaImpl.RedisMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.utils.BeanUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author zhanglea 16/9/9
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisMetaServiceTest extends AbstractConsoleTest{

	@Mock
	private RedisService redisService;
	@Mock
	private ClusterService clusterService;
	@Mock
	private DcService dcService;
	@Mock
	private ClusterMetaModifiedNotifier notifier;

	@InjectMocks
	private RedisMetaServiceImpl redisMetaService;

	private KeeperMeta mockedNewActiveKeeper;

	private RedisTbl mockedMasterRedis;

	private RedisTbl mockedActiveKeeper;
	private RedisTbl mockedBackupKeeperA;
	private RedisTbl mockedBackupKeeperB;

	private ClusterTbl mockedCluster;

	private DcTbl mockedDc;

	private String dc = "testDc", cluster = "testCluster", shard = "testShard";

	@Before
	public void initMockData() {
		mockedNewActiveKeeper = instanceOfKeeperMeta();

		mockedMasterRedis = instanceOfRedis(1, "masterRedis", "1.0.0.0", 0, XpipeConsoleConstant.ROLE_REDIS);

		mockedActiveKeeper = instanceOfRedis(2, "activeKeeper", "1.0.0.1", 1, XpipeConsoleConstant.ROLE_KEEPER);
		mockedActiveKeeper.setKeeperActive(true).setKeepercontainerId(2);

		mockedBackupKeeperA = instanceOfRedis(3, "backupKeeperA", "1.0.0.2", 2, XpipeConsoleConstant.ROLE_KEEPER);
		mockedBackupKeeperA.setKeeperActive(false).setKeepercontainerId(3).setRedisPort(6000);

		mockedBackupKeeperB = instanceOfRedis(4, "backupKeeperB", "1.0.0.3", 2, XpipeConsoleConstant.ROLE_KEEPER);
		mockedBackupKeeperB.setKeeperActive(false).setKeepercontainerId(4);

		mockedCluster = instanceOfCluster(1);

		mockedDc = instanceOfDc();

	}

	@Test
	public void testMasterDcKeepersChanged() {

		List<RedisTbl> redises = Arrays.asList(mockedActiveKeeper, mockedBackupKeeperA, mockedBackupKeeperB, mockedMasterRedis);
		List<RedisTbl> copedRedises = BeanUtils.batchTransform(RedisTbl.class, redises);

		when(redisService.findShardRedises(dc, cluster, shard)).thenReturn(copedRedises);
		when(clusterService.load(cluster)).thenReturn(mockedCluster);
		when(dcService.load(dc)).thenReturn(mockedDc);
		when(dcService.findClusterRelatedDc(anyString())).thenReturn(Collections.<DcTbl>emptyList());
		
		// keepA 2 active
		mockedNewActiveKeeper.setKeeperContainerId(mockedBackupKeeperA.getKeepercontainerId());
		redisMetaService.updateKeeperStatus(dc, cluster, shard, mockedNewActiveKeeper);

		RedisTbl newActiveKeeper = copedRedises.get(1);
		RedisTbl newBackupKeeperA = copedRedises.get(0);
		RedisTbl newBackupKeeperB = copedRedises.get(2);

		Assert.assertTrue(newActiveKeeper.isKeeperActive());
		Assert.assertEquals(mockedMasterRedis.getId(), newActiveKeeper.getRedisMaster());

		Assert.assertFalse(newBackupKeeperA.isKeeperActive());
		Assert.assertEquals(newActiveKeeper.getId(), newBackupKeeperA.getRedisMaster());

		Assert.assertFalse(newBackupKeeperB.isKeeperActive());
		Assert.assertEquals(newActiveKeeper.getId(), newBackupKeeperB.getRedisMaster());

		verify(redisService, times(1)).batchUpdate(RedisService.findWithRole(copedRedises, XpipeConsoleConstant.ROLE_KEEPER));
		verify(redisService, times(0)).updateByPK(any(RedisTbl.class));
		verify(notifier, times(1)).notifyUpstreamChanged(cluster, shard, newActiveKeeper.getRedisIp(), newActiveKeeper.getRedisPort(), Collections.<DcTbl>emptyList());
	}

	@Test
	public void testSlaveDcKeepersChanged() {
		List<RedisTbl> keepers = Arrays.asList(mockedActiveKeeper, mockedBackupKeeperA, mockedBackupKeeperB);
		List<RedisTbl> copedKeepers = BeanUtils.batchTransform(RedisTbl.class, keepers);
		DcTbl masterDc = instanceOfDc();
		long masterDcId = 3;
		String masterDcRunId = "masterDcRunId";
		masterDc.setId(masterDcId);
		masterDc.setDcName(masterDcRunId);

		mockedCluster.setActivedcId(masterDcId);
		long masterDcActiveKeeperId = 6;
		RedisTbl masterDcActiveKeeper = instanceOfRedis(masterDcActiveKeeperId, "masterDcActiveKeeper", "1.0.0.5", 1,
				XpipeConsoleConstant.ROLE_KEEPER).setKeeperActive(true);

		when(redisService.findShardRedises(dc, cluster, shard)).thenReturn(copedKeepers);
		when(clusterService.load(cluster)).thenReturn(mockedCluster);
		when(dcService.load(dc)).thenReturn(mockedDc);
		when(dcService.load(mockedCluster.getActivedcId())).thenReturn(masterDc);
		when(redisService.findShardRedises(masterDcRunId, mockedCluster.getClusterName(), shard))
			.thenReturn(Arrays.asList(masterDcActiveKeeper));

		// keepA 2 active
		mockedNewActiveKeeper.setKeeperContainerId(mockedBackupKeeperA.getKeepercontainerId());
		redisMetaService.updateKeeperStatus(dc, cluster, shard, mockedNewActiveKeeper);

		RedisTbl newActiveKeeper = copedKeepers.get(1);
		RedisTbl newBackupKeeperA = copedKeepers.get(0);
		RedisTbl newBackupKeeperB = copedKeepers.get(2);

		Assert.assertTrue(newActiveKeeper.isKeeperActive());
		Assert.assertEquals(masterDcActiveKeeper.getId(), newActiveKeeper.getRedisMaster());

		Assert.assertFalse(newBackupKeeperA.isKeeperActive());
		Assert.assertEquals(newActiveKeeper.getId(), newBackupKeeperA.getRedisMaster());

		Assert.assertFalse(newBackupKeeperB.isKeeperActive());
		Assert.assertEquals(newActiveKeeper.getId(), newBackupKeeperB.getRedisMaster());
	}

	public RedisTbl instanceOfRedis(long id, String runId, String ip, long master, String role) {
		RedisTbl redis = new RedisTbl();
		redis.setId(id);
		redis.setRunId(runId);
		redis.setRedisIp(ip);
		redis.setRedisMaster(master);
		redis.setRedisRole(role);
		return redis;
	}

	public ClusterTbl instanceOfCluster(long activeDcId) {
		ClusterTbl clusterTbl = new ClusterTbl();
		clusterTbl.setId(1);
		clusterTbl.setActivedcId(activeDcId);
		clusterTbl.setClusterName(cluster);
		return clusterTbl;
	}

	public DcTbl instanceOfDc() {
		DcTbl dcTbl = new DcTbl();
		dcTbl.setClusterName(cluster);
		dcTbl.setId(1);
		dcTbl.setDcActive(true);
		return dcTbl;
	}

	public KeeperMeta instanceOfKeeperMeta() {
		return new KeeperMeta();
	}

}
