package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.*;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

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
	private AdvancedDcMetaService mockedDcMetaService;
	@InjectMocks
	private ClusterMetaServiceImpl clusterMetaService;
	@InjectMocks
	private ShardMetaServiceImpl shardMetaService;
	
	@Test
	public void testKeepercontainerMetaService() {
		KeeperContainerMeta expect = new KeeperContainerMeta().setId(1L).setIp("1").setPort(1).setParent(null);
		
		KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl().setKeepercontainerId(1).setKeepercontainerIp("1").setKeepercontainerPort(1);
		
		assertEquals(expect, new KeepercontainerMetaServiceImpl().encodeKeepercontainerMeta(
				keepercontainerTbl, null, Collections.emptyMap()));
	}

	@Test
	public void testKeepercontainerMetaServiceWithHostname() {
		// 容器形态：IP 为空/0.0.0.0，hostname 非空
		KeeperContainerMeta expect = new KeeperContainerMeta()
				.setId(1L)
				.setIp("")
				.setPort(8080)
				.setHostname("pod-a-0")
				.setParent(null);
		
		KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl()
				.setKeepercontainerId(1)
				.setKeepercontainerIp("")
				.setKeepercontainerPort(8080)
				.setKeepercontainerHostName("pod-a-0");
		
		assertEquals(expect, new KeepercontainerMetaServiceImpl().encodeKeepercontainerMeta(
				keepercontainerTbl, null, Collections.emptyMap()));
	}

	@Test
	public void testKeepercontainerMetaServiceFillsTfsFsIdFromPreloadedMap() {
		KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl()
				.setKeepercontainerId(1).setKeepercontainerIp("1").setKeepercontainerPort(1)
				.setLogicalBuId(42L);
		Map<Long, String> logicalBuTfsFsIdById = new HashMap<>();
		logicalBuTfsFsIdById.put(42L, "fs-42");

		KeeperContainerMeta meta = new KeepercontainerMetaServiceImpl()
				.encodeKeepercontainerMeta(keepercontainerTbl, null, logicalBuTfsFsIdById);

		assertEquals(42L, meta.getLogicalBuId().longValue());
		assertEquals("fs-42", meta.getTfsFsId());
	}

	@Test
	public void testKeepercontainerMetaServiceSkipsTfsFsIdWhenMapMissing() {
		KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl()
				.setKeepercontainerId(1).setKeepercontainerIp("1").setKeepercontainerPort(1)
				.setLogicalBuId(42L);

		KeeperContainerMeta meta = new KeepercontainerMetaServiceImpl()
				.encodeKeepercontainerMeta(keepercontainerTbl, null, Collections.emptyMap());

		assertEquals(42L, meta.getLogicalBuId().longValue());
		assertNull(meta.getTfsFsId());
	}
	
	@Test
	public void testSetinelMetaService() {
		SentinelMeta expect = new SentinelMeta().setId(1L).setAddress("1").setParent(null);
		
		SentinelGroupModel setinelTbl = new SentinelGroupModel(new SentinelGroupTbl().setSentinelGroupId(1L)).addSentinel(new SentinelInstanceModel(new SentinelTbl().setSentinelIp("1")));
		assertEquals(expect,new SentinelMetaServiceImpl().encodeSetinelMeta(setinelTbl, null));
	}

	@Test
	public void testKeeperMetaWithHostname() {
		// 容器形态：redis_ip=0.0.0.0, host_name="pod-a-0"
		RedisTbl redisTbl = new RedisTbl()
				.setRunId("runid1")
				.setRedisIp("0.0.0.0")
				.setHostName("pod-a-0")
				.setRedisPort(6380)
				.setKeeperActive(true)
				.setKeepercontainerId(1L)
				.setKeeperPriority(100);

		KeeperMeta keeperMeta = new RedisMetaServiceImpl().getKeeperMeta(null, redisTbl);
		
		assertEquals("0.0.0.0", keeperMeta.getIp());   // 决策 1：容器 keeper 不带真实 IP
		assertEquals("pod-a-0", keeperMeta.getHostname());
		assertEquals(6380, keeperMeta.getPort().intValue());
		assertEquals("runid1", keeperMeta.getId());
		assertTrue(keeperMeta.getActive());
		assertEquals(Long.valueOf(1L), keeperMeta.getKeeperContainerId());
		assertEquals(Integer.valueOf(100), keeperMeta.getPriority());
	}

	@Test
	public void testKeeperMetaPhysicalMachine() {
		// 物理机形态：redis_ip 为真实 IP，host_name 为空串
		RedisTbl redisTbl = new RedisTbl()
				.setRunId("runid2")
				.setRedisIp("10.0.0.5")
				.setHostName("")
				.setRedisPort(6380)
				.setKeeperActive(false)
				.setKeepercontainerId(2L)
				.setKeeperPriority(50);

		KeeperMeta keeperMeta = new RedisMetaServiceImpl().getKeeperMeta(null, redisTbl);
		
		assertEquals("10.0.0.5", keeperMeta.getIp());
		assertEquals("", keeperMeta.getHostname());
		assertEquals(6380, keeperMeta.getPort().intValue());
		assertFalse(keeperMeta.getActive());
	}
	
	@Override
	protected String getXpipeMetaConfigFile() {
		return "metainfo.xml";
	}
}
