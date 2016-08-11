package com.ctrip.xpipe.redis.console.service;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.unidal.dal.jdbc.DalException;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;
import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTblEntity;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;

@RunWith(MockitoJUnitRunner.class)
public class BasicServiceTest extends AbstractRedisTest{
	@Mock
	private DcTblDao mockedDcTblDao;
	@InjectMocks
	private DcService dcService;
	@Mock
	private ClusterTblDao mockedClusterTblDao;
	@InjectMocks
	private ClusterService clusterService;
	@Mock
	private ShardTblDao mockedShardTblDao;
	@InjectMocks
	private ShardService shardService;
	@Mock
	private KeepercontainerTblDao mockedKeepercontainerTblDao;
	@InjectMocks
	private KeepercontainerService keepercontainerService;
	@Mock
	private SetinelTblDao mockedSetinelTblDao;
	@InjectMocks
	private SetinelService setinelService;
	@Mock
	private MetaserverTblDao mockedMetaserverTblDao;
	@InjectMocks
	private MetaserverService metaserverService;
	
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		
		try {
			generateDcMockData();
			generateClusterMockData();
			generateShardMockData();
			generateMetaMockData();
		} catch (Exception e) {
			logger.error("Generate Dc mock data failed.", e);
		}
	}
	
	@Test
	public void testDcService() {
		DcTbl target_result = new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC")
				.setDcLastModifiedTime("1234567");
		
		assertEquals(dcService.load("NTGXH").getId(), target_result.getId());
		assertEquals(dcService.load("NTGXH").getClusterName(), target_result.getClusterName());

	}
	
	@Test 
	public void testClusterService() {
		ClusterTbl target_result = new ClusterTbl().setId(1).setClusterName("cluster1").setClusterLastModifiedTime("1234567");
		
		assertEquals(clusterService.load("cluster1").getId(), target_result.getId());
		assertEquals(clusterService.load("cluster1").getClusterName(), target_result.getClusterName());
	}
	
	@Test
	public void testShardService() {
		ShardTbl target_result = new ShardTbl().setId(1).setClusterId(1).setShardName("shard1");
		
		assertEquals(shardService.load("cluster1", "shard1").getId(), target_result.getId());
		assertEquals(shardService.load("cluster1","shard1").getClusterId(), target_result.getClusterId());
		assertEquals(shardService.load("cluster1", "shard1").getShardName(), target_result.getShardName());
	}
	
	@Test
	public void testMetasService() {
		KeepercontainerTbl target_keepercontainer = new KeepercontainerTbl().setKeepercontainerId(1);
		SetinelTbl target_setinel = new SetinelTbl().setSetinelId(1).setSetinelAddress("11111");
		MetaserverTbl target_metaserver = new MetaserverTbl().setId(1).setMetaserverName("meta1");
		
		assertEquals(keepercontainerService.findByDcName("NTGXH").get(0).getKeepercontainerId(), target_keepercontainer.getKeepercontainerId());
		assertEquals(setinelService.findByDcName("NTGXH").get(0).getSetinelAddress(),target_setinel.getSetinelAddress());
		assertEquals(metaserverService.findByDcName("NTGXH").get(0).getMetaserverName(), target_metaserver.getMetaserverName());
	}
	
	private void generateDcMockData() throws DalException {
		when(mockedDcTblDao.findDcByDcName("NTGXH", DcTblEntity.READSET_FULL)).thenReturn(
				new DcTbl().setId(1).setDcName("NTGXH").setDcDescription("Mocked DC")
					.setDcLastModifiedTime("1234567"));
	}
	
	private void generateClusterMockData() throws DalException {
		when(mockedClusterTblDao.findClusterByClusterName("cluster1", ClusterTblEntity.READSET_FULL)).thenReturn(
				new ClusterTbl().setId(1).setClusterName("cluster1").setClusterLastModifiedTime("1234567"));
	}
	
	private void generateShardMockData() throws DalException {
		when(mockedShardTblDao.findShard("cluster1", "shard1", ShardTblEntity.READSET_FULL)).thenReturn(
				new ShardTbl().setId(1).setClusterId(1).setShardName("shard1"));
	}
	
	private void generateMetaMockData() throws DalException {
		when(mockedKeepercontainerTblDao.findByDcName("NTGXH", KeepercontainerTblEntity.READSET_FULL)).thenReturn(
				Arrays.asList(new KeepercontainerTbl().setKeepercontainerId(1)));
		when(mockedSetinelTblDao.findByDcName("NTGXH", SetinelTblEntity.READSET_FULL)).thenReturn(
				Arrays.asList(new SetinelTbl().setSetinelId(1).setSetinelAddress("11111")));
		when(mockedMetaserverTblDao.findByDcName("NTGXH", MetaserverTblEntity.READSET_FULL)).thenReturn(
				Arrays.asList(new MetaserverTbl().setId(1).setMetaserverName("meta1")));
	}
	
}
