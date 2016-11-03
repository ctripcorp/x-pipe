package com.ctrip.xpipe.redis.console.service;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.metaImpl.ClusterMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.metaImpl.DcMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.metaImpl.KeepercontainerMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.metaImpl.MetaserverMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.metaImpl.RedisMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.metaImpl.SentinelMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.metaImpl.ShardMetaServiceImpl;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.*;

import java.util.Arrays;
import java.util.HashMap;

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
	public void testShardMetaService() {
		XpipeMeta xpipeMeta = getXpipeMeta();
		ShardMeta shardMeta = xpipeMeta.getDcs().get("ntgxh").getClusters().get("cluster1")
				.getShards().get("shard1");
		
		DcTbl dcInfo = new DcTbl().setId(1L).setDcName("ntgxh");
		ClusterTbl clusterInfo = new ClusterTbl().setId(1L).setActivedcId(1);
		ShardTbl shardInfo = new ShardTbl().setId(1L).setShardName("shard1").setSetinelMonitorName("cluster1-shard1");
				
		assertEquals(shardMetaService.encodeShardMeta(dcInfo, clusterInfo, shardInfo, null), shardMeta);
	}
	
	@Test
	public void testClusterMetaService() {
		XpipeMeta xpipeMeta = getXpipeMeta();
		ClusterMeta clusterMeta = xpipeMeta.getDcs().get("ntgxh").getClusters().get("cluster1");
		
		assertEquals(clusterMetaService.getClusterMeta("ntgxh", "cluster1"), clusterMeta);
	}
	
	@Test
	public void testKeepercontainerMetaService() {
		KeeperContainerMeta expect = new KeeperContainerMeta().setId(1L).setIp("1").setPort(1).setParent(null);
		
		KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl().setKeepercontainerId(1).setKeepercontainerIp("1").setKeepercontainerPort(1);
		
		assertEquals(expect,new KeepercontainerMetaServiceImpl().encodeKeepercontainerMeta(keepercontainerTbl, null));
	}
	
	@Test
	public void testMetaserverMetaService() {
		MetaServerMeta expect = new MetaServerMeta().setIp("1").setPort(1).setMaster(true).setParent(null);
		
		MetaserverTbl metaserverTbl = new MetaserverTbl().setMetaserverIp("1").setMetaserverPort(1).setMetaserverRole("master");
		assertEquals(expect,new MetaserverMetaServiceImpl().encodeMetaserver(metaserverTbl, null));
	}
	
	@Test
	public void testSetinelMetaService() {
		SentinelMeta expect = new SentinelMeta().setId(1L).setAddress("1").setParent(null);
		
		SetinelTbl setinelTbl = new SetinelTbl().setSetinelId(1L).setSetinelAddress("1");
		assertEquals(expect,new SentinelMetaServiceImpl().encodeSetinelMeta(setinelTbl, null));
	}
	
	@Test
	public void testRedisMetaService() {
		String expect = "127.0.0.1:8080";
		RedisTbl redisTbl = new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(8080);
		
		assertEquals(expect, new RedisMetaServiceImpl().encodeRedisAddress(redisTbl));
	}
	
	@SuppressWarnings("unchecked")
	private void generateClusterMetaMockData() {
		when(mockedDcService.load("ntgxh")).thenReturn(new DcTbl().setId(1L).setDcName("ntgxh").setDcLastModifiedTime("1234567"));
		when(mockedDcService.load(1L)).thenReturn(new DcTbl().setId(1L).setDcName("ntgxh").setDcLastModifiedTime("1234567"));
		when(mockedClusterService.load("cluster1")).thenReturn(new ClusterTbl().setId(1).setClusterName("cluster1").setActivedcId(1L)
				.setClusterLastModifiedTime("1234567"));
		when(mockedDcClusterService.load("ntgxh", "cluster1")).thenReturn(new DcClusterTbl().setDcClusterId(1L)
				.setDcClusterPhase(1));
		when(mockedShardService.loadAllByClusterName("cluster1")).thenReturn(Arrays.asList(new ShardTbl()
				.setId(1L).setShardName("shard1").setSetinelMonitorName("cluster1-shard1")));
		
		when(dcMetaService.loadAllActiveKeepers()).thenReturn(new HashMap<Triple<Long, Long, Long>, RedisTbl>());
		
		when(mockedShardMetaService.encodeShardMeta(any(DcTbl.class), any(ClusterTbl.class), any(ShardTbl.class), any(HashMap.class))).thenReturn(
						new ShardMeta().setId("shard1").setPhase(1).setSentinelId(1L).setSentinelMonitorName("cluster1-shard1")
						.setUpstream(""));
	}
	
	private void generateShardMetaMockData() {
		when(mockedDcClusterService.load(1L, 1L)).thenReturn(new DcClusterTbl().setDcClusterId(1));
		when(mockedDcClusterShardService.load(1L, 1L)).thenReturn(new DcClusterShardTbl().setDcClusterId(1L)
				.setSetinelId(1).setDcClusterShardPhase(1));
		when(mockedRedisService.findByDcClusterShardId(1L)).thenReturn(Arrays.asList(
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
