package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.AzGroupModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
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

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;
import static org.mockito.ArgumentMatchers.any;
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
}
