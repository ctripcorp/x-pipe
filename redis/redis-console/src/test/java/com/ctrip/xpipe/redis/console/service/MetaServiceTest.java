package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.*;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

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
		
		assertEquals(expect,new KeepercontainerMetaServiceImpl().encodeKeepercontainerMeta(keepercontainerTbl, null));
	}
	
	@Test
	public void testSetinelMetaService() {
		SentinelMeta expect = new SentinelMeta().setId(1L).setAddress("1").setParent(null);
		
		SentinelGroupModel setinelTbl = new SentinelGroupModel(new SentinelGroupTbl().setSentinelGroupId(1L)).addSentinel(new SentinelInstanceModel(new SentinelTbl().setSentinelIp("1")));
		assertEquals(expect,new SentinelMetaServiceImpl().encodeSetinelMeta(setinelTbl, null));
	}
	
	@Override
	protected String getXpipeMetaConfigFile() {
		return "metainfo.xml";
	}
}
