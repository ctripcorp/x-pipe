package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.*;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MetaServiceTest extends AbstractConsoleTest{
	@Mock
	private DcTblDao dcTblDao;
	@Mock
	private DcService mockedDcService;
	@Mock
	private ClusterService mockedClusterService;
	@Mock
	private ShardService mockedShardService;
	@Mock
	private RedisService mockedRedisService;
	@Mock
	private DcClusterService mockedDcClusterService;
	@Mock
	private DcClusterShardService mockedDcClusterShardService;
	@Mock
	private DcMetaService dcMetaService;
	@Mock
	private ShardMetaServiceImpl mockedShardMetaService;
	@Mock
	private RedisMetaService mockedRedisMetaService;
	@InjectMocks
	private DcMetaServiceImpl mockedDcMetaService;
	@InjectMocks
	private ClusterMetaServiceImpl clusterMetaService;
	@InjectMocks
	private ShardMetaServiceImpl shardMetaService;
	
	@Test
	public void testKeepercontainerMetaService() {
		KeeperContainerMeta expect = new KeeperContainerMeta().setId(1L).setIp("1").setPort(1).setParent(null);
		
		KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl().setKeepercontainerId(1).setKeepercontainerIp("1").setKeepercontainerPort(1);
		
		assertEquals(expect,new KeepercontainerMetaServiceImpl().encodeKeepercontainerMeta(keepercontainerTbl, null));
	}
	
	@Test
	public void testSetinelMetaService() {
		SentinelMeta expect = new SentinelMeta().setId(1L).setAddress("1").setParent(null);
		
		SetinelTbl setinelTbl = new SetinelTbl().setSetinelId(1L).setSetinelAddress("1");
		assertEquals(expect,new SentinelMetaServiceImpl().encodeSetinelMeta(setinelTbl, null));
	}
	
	private void generateClusterMetaMockData() {
		when(mockedDcService.find("ntgxh")).thenReturn(new DcTbl().setId(1L).setDcName("ntgxh").setDcLastModifiedTime("1234567"));
		when(mockedDcService.find(1L)).thenReturn(new DcTbl().setId(1L).setDcName("ntgxh").setDcLastModifiedTime("1234567"));
		when(mockedClusterService.find("cluster1")).thenReturn(new ClusterTbl().setId(1).setClusterName("cluster1").setActivedcId(1L)
				.setClusterLastModifiedTime("1234567"));
		when(mockedDcClusterService.find("ntgxh", "cluster1")).thenReturn(new DcClusterTbl().setDcClusterId(1L)
				.setDcClusterPhase(1));
		when(mockedShardService.findAllByClusterName("cluster1")).thenReturn(Arrays.asList(new ShardTbl()
				.setId(1L).setShardName("shard1").setSetinelMonitorName("cluster1-shard1")));
		
		when(mockedShardMetaService.getShardMeta(any(DcTbl.class), any(ClusterTbl.class), any(ShardTbl.class))).thenReturn(
						new ShardMeta().setId("shard1").setPhase(1).setSentinelId(1L).setSentinelMonitorName("cluster1-shard1"));
	}
	
	private void generateShardMetaMockData() {
		when(mockedDcClusterService.find(1L, 1L)).thenReturn(new DcClusterTbl().setDcClusterId(1));
		when(mockedDcClusterShardService.find(1L, 1L)).thenReturn(new DcClusterShardTbl().setDcClusterId(1L)
				.setSetinelId(1).setDcClusterShardPhase(1));
		when(mockedRedisService.findAllByDcClusterShard(1L)).thenReturn(Arrays.asList(
				new RedisTbl().setId(1L).setRunId("40a").setRedisIp("1.1.1.1").setRedisPort(8888).setRedisMaster(2L).setKeeperActive(true)
					.setKeepercontainerId(1L),
				new RedisTbl().setId(2L).setRunId("40b").setRedisIp("1.1.1.3").setRedisPort(1234)));
	}
	
	@Override
	protected String getXpipeMetaConfigFile() {
		return "metainfo.xml";
	}

	@Before
	public void initMockData() throws Exception {
		generateClusterMetaMockData();
		generateShardMetaMockData();
	}
}
