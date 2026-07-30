package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.entity.DcClusterEntity;
import com.ctrip.xpipe.redis.console.entity.ShardEntity;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.repository.DcClusterRepository;
import com.ctrip.xpipe.redis.console.repository.ShardRepository;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ShardServiceImplTest3 {

	@Mock
	private RedisService redisService;

	@Mock
	private KeeperAdvancedService keeperAdvancedService;

	@Mock
	private DcClusterService dcClusterService;

	@Mock
	private AzGroupClusterRepository azGroupClusterRepository;

	@Mock
	private AzGroupCache azGroupCache;

	@Mock
	private ClusterDao clusterDao;

	@Mock
	private ShardRepository shardRepository;

	@Mock
	private DcClusterRepository dcClusterRepository;

	@Mock
	private DcClusterShardService dcClusterShardService;

	@Mock
	private SentinelBalanceService sentinelBalanceService;

	@Mock
	private DcService dcService;

	@InjectMocks
	private ShardServiceImpl shardService = new ShardServiceImpl();

	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	// ===== validateRedisCreateInfo tests =====

	@Test
	public void testValidateRedisCreateInfoNoDuplicate() {
		List<RedisCreateInfo> infos = Lists.newArrayList(
			new RedisCreateInfo().setDcId("jq"),
			new RedisCreateInfo().setDcId("oy")
		);
		shardService.validateRedisCreateInfo(infos);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testValidateRedisCreateInfoWithDuplicate() {
		List<RedisCreateInfo> infos = Lists.newArrayList(
			new RedisCreateInfo().setDcId("jq"),
			new RedisCreateInfo().setDcId("jq")
		);
		shardService.validateRedisCreateInfo(infos);
	}

	@Test
	public void testValidateRedisCreateInfoEmptyList() {
		shardService.validateRedisCreateInfo(Collections.emptyList());
	}

	@Test
	public void testValidateRedisCreateInfoWithDcClusters() {
		List<RedisCreateInfo> infos = Lists.newArrayList(
			new RedisCreateInfo().setDcId("jq"),
			new RedisCreateInfo().setDcId("oy")
		);
		DcClusterEntity dcCluster1 = mock(DcClusterEntity.class);
		when(dcCluster1.getDcId()).thenReturn(1L);
		DcClusterEntity dcCluster2 = mock(DcClusterEntity.class);
		when(dcCluster2.getDcId()).thenReturn(2L);
		when(dcService.getDcName(1L)).thenReturn("jq");
		when(dcService.getDcName(2L)).thenReturn("oy");
		shardService.validateRedisCreateInfo(infos, Lists.newArrayList(dcCluster1, dcCluster2));
	}

	@Test(expected = BadRequestException.class)
	public void testValidateRedisCreateInfoDcNotInRegion() {
		List<RedisCreateInfo> infos = Lists.newArrayList(
			new RedisCreateInfo().setDcId("fra")
		);
		DcClusterEntity dcCluster1 = mock(DcClusterEntity.class);
		when(dcCluster1.getDcId()).thenReturn(1L);
		when(dcService.getDcName(1L)).thenReturn("jq");
		shardService.validateRedisCreateInfo(infos, Lists.newArrayList(dcCluster1));
	}

	@Test
	public void testValidateRedisCreateInfoWithNullDcClusters() {
		List<RedisCreateInfo> infos = Lists.newArrayList(
			new RedisCreateInfo().setDcId("jq"),
			new RedisCreateInfo().setDcId("oy")
		);
		// null azGroupDcClusters — skip region check, same as 1-param version
		shardService.validateRedisCreateInfo(infos, null);
	}

	// ===== addRedises tests =====

	@Test
	public void testAddRedises() throws Exception {
		ClusterTbl clusterTbl = mock(ClusterTbl.class);
		when(clusterTbl.getClusterName()).thenReturn("cluster-test");

		RedisCreateInfo info1 = new RedisCreateInfo().setDcId("jq").setRedises("127.0.0.1:6379");
		RedisCreateInfo info2 = new RedisCreateInfo().setDcId("oy").setRedises("127.0.0.2:6379");

		shardService.addRedises(clusterTbl, "shard1", Lists.newArrayList(info1, info2));

		verify(redisService).insertRedises(eq("jq"), eq("cluster-test"), eq("shard1"), any(Map.class));
		verify(redisService).insertRedises(eq("oy"), eq("cluster-test"), eq("shard1"), any(Map.class));
	}

	// ===== addKeepers tests =====

	@Test
	public void testAddKeepersNonKeeperClusterType() throws Exception {
		ClusterTbl clusterTbl = mock(ClusterTbl.class);
		when(clusterTbl.getClusterType()).thenReturn(ClusterType.SINGLE_DC.toString());

		RedisCreateInfo info = new RedisCreateInfo().setDcId("jq");
		shardService.addKeepers(clusterTbl, "shard1", Lists.newArrayList(info));

		verify(keeperAdvancedService, never()).findBestKeepers(anyString(), any(int.class), any(), anyString());
	}

	@Test
	public void testAddKeepersWithKeeperSupport() throws Exception {
		ClusterTbl clusterTbl = mock(ClusterTbl.class);
		when(clusterTbl.getClusterName()).thenReturn("cluster-test");
		when(clusterTbl.getId()).thenReturn(1L);
		when(clusterTbl.getClusterType()).thenReturn(ClusterType.ONE_WAY.toString());

		AzGroupClusterEntity azGroupCluster = mock(AzGroupClusterEntity.class);
		when(azGroupCluster.getAzGroupClusterType()).thenReturn(ClusterType.ONE_WAY.toString());
		when(azGroupClusterRepository.selectByClusterId(1L)).thenReturn(Lists.newArrayList(azGroupCluster));

		AzGroupModel azGroup = mock(AzGroupModel.class);
		when(azGroup.getAzs()).thenReturn(Sets.newHashSet("jq", "oy"));
		when(azGroupCache.getAzGroupById(anyLong())).thenReturn(azGroup);

		DcClusterTbl dcClusterTbl = mock(DcClusterTbl.class);
		when(dcClusterService.find("jq", "cluster-test")).thenReturn(dcClusterTbl);

		RedisCreateInfo info = new RedisCreateInfo().setDcId("jq");
		ShardServiceImpl spyService = spy(shardService);
		doReturn(0).when(spyService).doAddKeepers(eq("jq"), eq("cluster-test"), eq("shard1"), eq("jq"));

		spyService.addKeepers(clusterTbl, "shard1", Lists.newArrayList(info));

		verify(spyService).doAddKeepers("jq", "cluster-test", "shard1", "jq");
	}

	@Test
	public void testAddKeepersSkipsSingleDc() throws Exception {
		ClusterTbl clusterTbl = mock(ClusterTbl.class);
		when(clusterTbl.getClusterName()).thenReturn("cluster-test");
		when(clusterTbl.getId()).thenReturn(1L);
		when(clusterTbl.getClusterType()).thenReturn(ClusterType.ONE_WAY.toString());

		AzGroupClusterEntity singleDcAzGroupCluster = mock(AzGroupClusterEntity.class);
		when(singleDcAzGroupCluster.getAzGroupClusterType()).thenReturn(ClusterType.SINGLE_DC.toString());
		when(azGroupClusterRepository.selectByClusterId(1L)).thenReturn(Lists.newArrayList(singleDcAzGroupCluster));

		AzGroupModel azGroup = mock(AzGroupModel.class);
		when(azGroup.getAzs()).thenReturn(Sets.newHashSet("fra"));
		when(azGroupCache.getAzGroupById(anyLong())).thenReturn(azGroup);

		DcClusterTbl dcClusterTbl = mock(DcClusterTbl.class);
		when(dcClusterService.find("fra", "cluster-test")).thenReturn(dcClusterTbl);

		RedisCreateInfo info = new RedisCreateInfo().setDcId("fra");
		ShardServiceImpl spyService = spy(shardService);

		spyService.addKeepers(clusterTbl, "shard1", Lists.newArrayList(info));

		verify(spyService, never()).doAddKeepers(anyString(), anyString(), anyString(), anyString());
	}

	@Test(expected = BadRequestException.class)
	public void testAddKeepersDcNotExistInCluster() throws Exception {
		ClusterTbl clusterTbl = mock(ClusterTbl.class);
		when(clusterTbl.getClusterName()).thenReturn("cluster-test");
		when(clusterTbl.getId()).thenReturn(1L);
		when(clusterTbl.getClusterType()).thenReturn(ClusterType.ONE_WAY.toString());

		when(azGroupClusterRepository.selectByClusterId(1L)).thenReturn(Collections.emptyList());
		when(dcClusterService.find("jq", "cluster-test")).thenReturn(null);

		RedisCreateInfo info = new RedisCreateInfo().setDcId("jq");
		shardService.addKeepers(clusterTbl, "shard1", Lists.newArrayList(info));
	}

	// ===== doAddKeepers tests =====

	@Test
	public void testDoAddKeepersNoExistingKeepers() throws Exception {
		String dc = "jq", cluster = "cluster-test", shard = "shard1";
		when(redisService.findKeepersByDcClusterShard(dc, cluster, shard))
			.thenThrow(new ResourceNotFoundException(dc, cluster, shard));

		KeeperBasicInfo keeper1 = new KeeperBasicInfo();
		keeper1.setHost("127.0.0.1");
		keeper1.setPort(6379);
		keeper1.setKeeperContainerId(1);
		KeeperBasicInfo keeper2 = new KeeperBasicInfo();
		keeper2.setHost("127.0.0.2");
		keeper2.setPort(6379);
		keeper2.setKeeperContainerId(2);

		List<KeeperBasicInfo> bestKeepers = Lists.newArrayList(keeper1, keeper2);
		when(keeperAdvancedService.findBestKeepers(eq(dc), eq(KEEPER_PORT_DEFAULT), any(), eq(cluster)))
			.thenReturn(bestKeepers);
		when(redisService.insertKeepers(dc, cluster, shard, bestKeepers)).thenReturn(2);

		int result = shardService.doAddKeepers(dc, cluster, shard, dc);
		Assert.assertEquals(2, result);
	}

	@Test
	public void testDoAddKeepersWithOneExistingKeeper() throws Exception {
		String dc = "jq", cluster = "cluster-test", shard = "shard1";
		RedisTbl existingKeeper = mock(RedisTbl.class);
		when(redisService.findKeepersByDcClusterShard(dc, cluster, shard))
			.thenReturn(Lists.newArrayList(existingKeeper));

		KeeperBasicInfo keeper1 = new KeeperBasicInfo();
		keeper1.setHost("127.0.0.1");
		keeper1.setPort(6379);
		keeper1.setKeeperContainerId(1);
		KeeperBasicInfo keeper2 = new KeeperBasicInfo();
		keeper2.setHost("127.0.0.2");
		keeper2.setPort(6379);
		keeper2.setKeeperContainerId(2);

		List<KeeperBasicInfo> bestKeepers = Lists.newArrayList(keeper1, keeper2);
		when(keeperAdvancedService.findBestKeepers(eq(dc), eq(KEEPER_PORT_DEFAULT), any(), eq(cluster)))
			.thenReturn(bestKeepers);
		when(redisService.insertKeepers(dc, cluster, shard, bestKeepers)).thenReturn(2);

		int result = shardService.doAddKeepers(dc, cluster, shard, dc);
		Assert.assertEquals(2, result);
		verify(redisService).deleteKeepers(dc, cluster, shard);
	}

	@Test
	public void testDoAddKeepersWithTwoExistingKeepers() throws Exception {
		String dc = "jq", cluster = "cluster-test", shard = "shard1";
		RedisTbl existing1 = mock(RedisTbl.class);
		RedisTbl existing2 = mock(RedisTbl.class);
		when(redisService.findKeepersByDcClusterShard(dc, cluster, shard))
			.thenReturn(Lists.newArrayList(existing1, existing2));

		int result = shardService.doAddKeepers(dc, cluster, shard, dc);
		Assert.assertEquals(0, result);
		verify(keeperAdvancedService, never()).findBestKeepers(anyString(), any(int.class), any(), anyString());
	}

	@Test(expected = IllegalStateException.class)
	public void testDoAddKeepersWithMoreThanTwoExistingKeepers() throws Exception {
		String dc = "jq", cluster = "cluster-test", shard = "shard1";
		when(redisService.findKeepersByDcClusterShard(dc, cluster, shard))
			.thenReturn(Lists.newArrayList(mock(RedisTbl.class), mock(RedisTbl.class), mock(RedisTbl.class)));

		shardService.doAddKeepers(dc, cluster, shard, dc);
	}

	// ===== createRegionShard tests =====

	private ClusterTbl clusterTbl;
	private AzGroupClusterEntity azGroupCluster;
	private long clusterId = 1L;

	private void setupCreateRegionShardMocks(String regionName) {
		clusterTbl = mock(ClusterTbl.class);
		when(clusterTbl.getId()).thenReturn(clusterId);
		when(clusterTbl.getClusterName()).thenReturn("cluster-test");
		when(clusterTbl.getClusterType()).thenReturn(ClusterType.ONE_WAY.toString());
		when(clusterDao.findClusterByClusterName("cluster-test")).thenReturn(clusterTbl);

		azGroupCluster = mock(AzGroupClusterEntity.class);
		when(azGroupCluster.getId()).thenReturn(1L);
		when(azGroupCluster.getAzGroupId()).thenReturn(10L);
		when(azGroupCluster.getAzGroupClusterType()).thenReturn(ClusterType.ONE_WAY.toString());
		when(azGroupClusterRepository.selectByClusterIdAndRegion(clusterId, regionName)).thenReturn(azGroupCluster);

		DcClusterEntity dcCluster1 = mock(DcClusterEntity.class);
		when(dcCluster1.getDcClusterId()).thenReturn(100L);
		when(dcCluster1.getDcId()).thenReturn(1L);
		DcClusterEntity dcCluster2 = mock(DcClusterEntity.class);
		when(dcCluster2.getDcClusterId()).thenReturn(101L);
		when(dcCluster2.getDcId()).thenReturn(2L);
		when(dcClusterRepository.selectByAzGroupClusterId(1L)).thenReturn(Lists.newArrayList(dcCluster1, dcCluster2));

		when(sentinelBalanceService.selectMultiDcSentinels(any(ClusterType.class), anyString()))
			.thenReturn(Collections.emptyMap());

		when(dcService.getDcName(1L)).thenReturn("jq");
		when(dcService.getDcName(2L)).thenReturn("oy");
	}

	// Case 1: Duplicate call — shard already exists, should skip creation
	@Test
	public void testCreateRegionShardDuplicateCall() throws Exception {
		setupCreateRegionShardMocks("SHA");

		ShardEntity existingShard = mock(ShardEntity.class);
		when(existingShard.getShardName()).thenReturn("shard1");
		when(existingShard.getSetinelMonitorName()).thenReturn("shard1");
		when(existingShard.getId()).thenReturn(100L);
		when(shardRepository.selectByAzGroupClusterId(1L)).thenReturn(Lists.newArrayList(existingShard));

		// dc_cluster_shard already exists
		when(dcClusterShardService.find(anyLong(), anyLong())).thenReturn(mock(DcClusterShardTbl.class));

		ShardServiceImpl spyService = spy(shardService);
		doNothing().when(spyService).addRedises(any(ClusterTbl.class), anyString(), any());
		doNothing().when(spyService).addKeepers(any(ClusterTbl.class), anyString(), any());

		RedisCreateInfo info = new RedisCreateInfo().setDcId("jq").setRedises("127.0.0.1:6379");
		spyService.createRegionShard("cluster-test", "SHA", "shard1", Lists.newArrayList(info));

		// No new shard inserted, no dc_cluster_shard inserted
		verify(shardRepository, never()).insert(any(ShardEntity.class));
		verify(dcClusterShardService, never()).insertBatch(anyList());
		// addRedises/addKeepers still called
		verify(spyService).addRedises(any(ClusterTbl.class), eq("shard1"), any());
		verify(spyService).addKeepers(any(ClusterTbl.class), eq("shard1"), any());
	}

	// Case 2: Create shard first, then add redis — second call should succeed
	@Test
	public void testCreateRegionShardFirstCreateThenAddRedis() throws Exception {
		setupCreateRegionShardMocks("SHA");

		// First call: no existing shard
		when(shardRepository.selectByAzGroupClusterId(1L)).thenReturn(Collections.emptyList());
		when(dcClusterShardService.find(anyLong(), anyLong())).thenReturn(mock(DcClusterShardTbl.class));

		doAnswer(invocation -> {
			ShardEntity shard = invocation.getArgument(0);
			shard.setId(100L);
			return 1;
		}).when(shardRepository).insert(any(ShardEntity.class));

		ShardServiceImpl spyService = spy(shardService);
		doNothing().when(spyService).addRedises(any(ClusterTbl.class), anyString(), any());
		doNothing().when(spyService).addKeepers(any(ClusterTbl.class), anyString(), any());

		// First call: create shard without redis
		spyService.createRegionShard("cluster-test", "SHA", "shard1", null);

		verify(shardRepository).insert(any(ShardEntity.class));
		verify(dcClusterShardService, never()).insertBatch(anyList());
		verify(spyService, never()).addRedises(any(ClusterTbl.class), anyString(), any());

		// Second call: shard now exists, add redis
		ShardEntity existingShard = mock(ShardEntity.class);
		when(existingShard.getShardName()).thenReturn("shard1");
		when(existingShard.getSetinelMonitorName()).thenReturn("shard1");
		when(existingShard.getId()).thenReturn(100L);
		when(shardRepository.selectByAzGroupClusterId(1L)).thenReturn(Lists.newArrayList(existingShard));
		when(dcClusterShardService.find(anyLong(), anyLong())).thenReturn(mock(DcClusterShardTbl.class));

		RedisCreateInfo info = new RedisCreateInfo().setDcId("jq").setRedises("127.0.0.1:6379");
		spyService.createRegionShard("cluster-test", "SHA", "shard1", Lists.newArrayList(info));

		verify(shardRepository, times(1)).insert(any(ShardEntity.class));
		verify(spyService).addRedises(any(ClusterTbl.class), eq("shard1"), any());
		verify(spyService).addKeepers(any(ClusterTbl.class), eq("shard1"), any());
	}

	// Case 3: dc_cluster_shard already exists — should skip insertion
	@Test
	public void testCreateRegionShardDcClusterShardAlreadyExists() throws Exception {
		setupCreateRegionShardMocks("SHA");

		when(shardRepository.selectByAzGroupClusterId(1L)).thenReturn(Collections.emptyList());

		doAnswer(invocation -> {
			ShardEntity shard = invocation.getArgument(0);
			shard.setId(100L);
			return 1;
		}).when(shardRepository).insert(any(ShardEntity.class));

		when(dcClusterShardService.find(anyLong(), anyLong())).thenReturn(mock(DcClusterShardTbl.class));

		shardService.createRegionShard("cluster-test", "SHA", "shard1", null);

		verify(shardRepository).insert(any(ShardEntity.class));
		verify(dcClusterShardService, never()).insertBatch(anyList());
	}

	// Case 4: Monitor name conflict with different shard — should throw
	@Test(expected = BadRequestException.class)
	public void testCreateRegionShardMonitorNameConflict() throws Exception {
		setupCreateRegionShardMocks("SHA");

		ShardEntity otherShard = mock(ShardEntity.class);
		when(otherShard.getShardName()).thenReturn("other-shard");
		when(otherShard.getSetinelMonitorName()).thenReturn("shard1");
		when(shardRepository.selectByAzGroupClusterId(1L)).thenReturn(Lists.newArrayList(otherShard));

		shardService.createRegionShard("cluster-test", "SHA", "shard1", null);
	}

	// Case 5: Same-name shard in another region is allowed — selectByAzGroupClusterId only returns shards in target region
	@Test
	public void testCreateRegionShardSameNameInOtherRegionAllowed() throws Exception {
		setupCreateRegionShardMocks("SHA");

		// No shard in SHA region (selectByAzGroupClusterId only returns shards in target region)
		when(shardRepository.selectByAzGroupClusterId(1L)).thenReturn(Collections.emptyList());

		doAnswer(invocation -> {
			ShardEntity shard = invocation.getArgument(0);
			shard.setId(100L);
			return 1;
		}).when(shardRepository).insert(any(ShardEntity.class));

		when(dcClusterShardService.find(anyLong(), anyLong())).thenReturn(mock(DcClusterShardTbl.class));

		// Should succeed: same-name shard in another region does not block creation
		shardService.createRegionShard("cluster-test", "SHA", "shard1", null);
		verify(shardRepository).insert(any(ShardEntity.class));
	}

	// Case 6: RedisCreateInfo dcId not in current region — should throw (validation in step2, before shard creation)
	@Test(expected = BadRequestException.class)
	public void testCreateRegionShardDcNotInRegion() throws Exception {
		setupCreateRegionShardMocks("SHA");

		// "fra" is not in SHA region's DCs (SHA has jq, oy)
		RedisCreateInfo info = new RedisCreateInfo().setDcId("fra").setRedises("10.0.0.1:6379");
		shardService.createRegionShard("cluster-test", "SHA", "shard1", Lists.newArrayList(info));
	}

	// Case 7: Region not found in cluster — should throw
	@Test(expected = BadRequestException.class)
	public void testCreateRegionShardRegionNotFound() throws Exception {
		setupCreateRegionShardMocks("SHA");

		// "FRA" region does not exist in this cluster (setup only has SHA region)
		when(azGroupClusterRepository.selectByClusterIdAndRegion(clusterId, "FRA")).thenReturn(null);
		shardService.createRegionShard("cluster-test", "FRA", "shard1", null);
	}
}
