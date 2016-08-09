package com.ctrip.xpipe.redis.console.service;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import org.mockito.runners.*;
import org.mockito.MockitoAnnotations;
import org.unidal.dal.jdbc.DalException;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.model.DcClusterTblEntity;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.DcTblDao;
import com.ctrip.xpipe.redis.console.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblEntity;
import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.SetinelTblEntity;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTblEntity;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;

/**
 * @author shyin
 *
 * Aug 8, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaInfoServiceTest extends AbstractRedisTest{
	@Mock
	private DcTblDao mockedDcTblDao;
	@Mock
	private ClusterTblDao mockedClusterTblDao;
	@Mock
	private DcClusterTblDao mockedDcClusterTblDao;
	@Mock
	private ShardTblDao mockedShardTblDao;
	@Mock
	private DcClusterShardTblDao mockedDcClusterShardTblDao;
	@Mock
	private RedisTblDao mockedRedisTblDao;
	@Mock
	private SetinelTblDao mockedSetinelTblDao;
	@Mock
	private MetaserverTblDao mockedMetaserverTblDao;
	@Mock
	private KeepercontainerTblDao mockedKeepercontainerTblDao;
	
	@InjectMocks
	private MetaInfoService defaultMetaInfoService;
	
	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
		
		try {
			generateMockData();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	@Test
	public void testGetAllDcIds() throws DalException {
		List<String> all_dcids = defaultMetaInfoService.getAllDcIds();
		
		assertEquals(Arrays.asList("jq","oy"),all_dcids);
	}
	
	@Test
	public void testGetAllClusterIds() throws DalException {
		List<String> all_clusterids = defaultMetaInfoService.getAllClusterIds();
		
		assertEquals(Arrays.asList("cluster1","cluster2"),all_clusterids);
	}
	
	@Test
	public void testGetAllClusterShardIds() throws DalException {
		List<String> all_cluster1_shard_ids = defaultMetaInfoService.getAllClusterShardIds("cluster1");
		
		assertEquals(Arrays.asList("shard1","shard2"),all_cluster1_shard_ids);
	}
	
	@Test
	public void testGetDcClusterShardMeta() throws DalException {
		XpipeMeta xpipeMeta = getXpipeMeta();
		ShardMeta shardMeta = xpipeMeta.getDcs().get("jq").getClusters().get("cluster1").getShards().get("shard1");
		
		assertEquals(shardMeta,defaultMetaInfoService.getDcClusterShardMeta("jq", "cluster1", "shard1"));
	}
	
	@Test
	public void testGetDcClusterMeta() throws DalException {
		XpipeMeta xpipeMeta = getXpipeMeta();
		ClusterMeta clusterMeta = xpipeMeta.getDcs().get("jq").getClusters().get("cluster1");
		
		assertEquals(clusterMeta,defaultMetaInfoService.getDcClusterMeta("jq", "cluster1"));
	}
	
	@Test
	public void testGetDcMeta() throws DalException {
		XpipeMeta xpipeMeta = getXpipeMeta();
		DcMeta dcMeta = xpipeMeta.getDcs().get("jq");
		
		assertEquals(dcMeta,defaultMetaInfoService.getDcMeta("jq"));
	}

	private void generateMockData() throws Exception {
		/** DcTbl **/
		DcTbl dc_jq = new DcTbl();
		DcTbl dc_oy = new DcTbl();
		dc_jq.setKeyId(1).setId(1).setDcName("jq").setDcActive(true).setDcDescription("dc named jq located in shanghai.")
			.setDcLastModifiedTime("1234567").setDataChangeLastTime(new Date());
		dc_oy.setKeyId(2).setId(2).setDcName("oy").setDcActive(true).setDcDescription("dc named oy located in shanghai.")
			.setDcLastModifiedTime("1234567").setDataChangeLastTime(new Date());
		
		/** ClusterTbl **/
		ClusterTbl cluster_1 = new ClusterTbl();
		ClusterTbl cluster_2 = new ClusterTbl();
		cluster_1.setKeyId(1).setId(1).setClusterName("cluster1").setActivedcId(1).setClusterDescription("Cluster 1 located in JQ")
			.setClusterLastModifiedTime("1234567").setDataChangeLastTime(new Date());
		cluster_2.setKeyId(2).setId(2).setClusterName("cluster2").setActivedcId(2).setClusterDescription("Cluster 2 located in OY")
			.setClusterLastModifiedTime("1234567").setDataChangeLastTime(new Date());
		
		/** ShardTbl **/
		ShardTbl shard_1 = new ShardTbl();
		ShardTbl shard_2 = new ShardTbl();
		shard_1.setKeyId(1).setId(1).setShardName("shard1").setClusterId(1).setDataChangeLastTime(new Date());
		shard_2.setKeyId(2).setId(2).setShardName("shard2").setClusterId(1).setDataChangeLastTime(new Date());
		
		/** DcClusterTbl **/
		DcClusterTbl dc_cluster_1 = new DcClusterTbl();
		DcClusterTbl dc_cluster_2 = new DcClusterTbl();
		dc_cluster_1.setKeyDcClusterId(1).setDcClusterId(1).setDcId(1).setClusterId(1).setMetaserverId(1).setDcClusterPhase(1)
			.setDataChangeLastTime(new Date());
		dc_cluster_2.setKeyDcClusterId(2).setDcClusterId(2).setDcId(2).setClusterId(1).setMetaserverId(3).setDcClusterPhase(1)
			.setDataChangeLastTime(new Date());
		
		/** DcClusterShardTbl **/
		DcClusterShardTbl dc_cluster_shard_1 = new DcClusterShardTbl();
		DcClusterShardTbl dc_cluster_shard_2 = new DcClusterShardTbl();
		dc_cluster_shard_1.setKeyDcClusterShardId(1).setDcClusterShardId(1).setDcClusterId(1).setShardId(1).setSetinelId(1)
			.setDcClusterShardPhase(1).setDataChangeLastTime(new Date());
		dc_cluster_shard_2.setKeyDcClusterShardId(2).setDcClusterShardId(2).setDcClusterId(2).setShardId(1).setSetinelId(2)
			.setDcClusterShardPhase(1).setDataChangeLastTime(new Date());
		
		/** RedisTbl **/
		RedisTbl redis_1 = new RedisTbl();
		RedisTbl redis_2 = new RedisTbl();
		RedisTbl redis_3 = new RedisTbl();
		RedisTbl redis_4 = new RedisTbl();
		redis_1.setKeyId(1).setId(1).setRedisName("40a").setDcClusterShardId(1).setRedisIp("1.1.1.1").setRedisPort(8888).setRedisRole("keeper")
			.setKeeperActive(true).setRedisMaster(3).setKeepercontainerId(1).setDataChangeLastTime(new Date());
		redis_2.setKeyId(2).setId(2).setRedisName("40b").setDcClusterShardId(1).setRedisIp("1.1.1.2").setRedisPort(9999).setRedisRole("keeper")
			.setKeeperActive(false).setRedisMaster(1).setKeepercontainerId(1).setDataChangeLastTime(new Date());
		redis_3.setKeyId(3).setId(3).setRedisName("40c").setDcClusterShardId(1).setRedisIp("1.1.1.3").setRedisPort(1234).setRedisRole("redis")
			.setKeeperActive(false).setRedisMaster(0).setKeepercontainerId(0).setDataChangeLastTime(new Date());
		redis_4.setKeyId(4).setId(4).setRedisName("40d").setDcClusterShardId(1).setRedisIp("1.1.1.4").setRedisPort(1234).setRedisRole("keeper")
			.setKeeperActive(false).setRedisMaster(3).setKeepercontainerId(0).setDataChangeLastTime(new Date());
		
		/** MetaServerTbl **/
		MetaserverTbl meta_1 = new MetaserverTbl();
		MetaserverTbl meta_2 = new MetaserverTbl();
		MetaserverTbl meta_3 = new MetaserverTbl();
		meta_1.setKeyId(1).setId(1).setMetaserverName("jq_meta_1").setDcId(1).setMetaserverIp("1.1.1.1").setMetaserverPort(9747)
			.setMetaserverActive(true).setMetaserverRole("master").setDataChangeLastTime(new Date());
		meta_2.setKeyId(2).setId(2).setMetaserverName("jq_meta_2").setDcId(1).setMetaserverIp("1.1.1.1").setMetaserverPort(9748)
			.setMetaserverActive(true).setMetaserverRole("slave").setDataChangeLastTime(new Date());
		meta_3.setKeyId(3).setId(3).setMetaserverName("oy_meta_2").setDcId(2).setMetaserverIp("1.1.1.1").setMetaserverPort(9749)
			.setMetaserverActive(true).setMetaserverRole("master").setDataChangeLastTime(new Date());
		
		/** KeepercontainerTbl **/
		KeepercontainerTbl keepercontainer_1 = new KeepercontainerTbl();
		KeepercontainerTbl keepercontainer_2 = new KeepercontainerTbl();
		keepercontainer_1.setKeyKeepercontainerId(1).setKeepercontainerId(1).setKeepercontainerDc(1).setKeepercontainerIp("1.1.1.1")
			.setKeepercontainerPort(8080).setKeepercontainerActive(true).setDataChangeLastTime(new Date());
		keepercontainer_2.setKeyKeepercontainerId(2).setKeepercontainerId(2).setKeepercontainerDc(1).setKeepercontainerIp("1.1.1.1")
			.setKeepercontainerPort(8081).setKeepercontainerActive(true).setDataChangeLastTime(new Date());
		
		/** SetinelTbl **/
		SetinelTbl setinel_1 = new SetinelTbl();
		SetinelTbl setinel_2 = new SetinelTbl();
		setinel_1.setKeySetinelId(1).setSetinelId(1).setDcId(1).setSetinelAddress("127.0.0.1:17171,127.0.0.1:17171")
			.setSetinelDescription("setinel no.1").setDataChangeLastTime(new Date());
		setinel_2.setKeySetinelId(2).setSetinelId(2).setDcId(1).setSetinelAddress("127.0.0.1:17171,127.0.0.1:17171")
			.setSetinelDescription("setinel no.2").setDataChangeLastTime(new Date());
		
		
		
		when(mockedDcTblDao.findDcByDcName("jq", DcTblEntity.READSET_FULL)).thenReturn(dc_jq);
		when(mockedDcTblDao.findDcByDcName("oy", DcTblEntity.READSET_FULL)).thenReturn(dc_oy);
		when(mockedDcTblDao.findByPK(1, DcTblEntity.READSET_FULL)).thenReturn(dc_jq);
		when(mockedDcTblDao.findByPK(2, DcTblEntity.READSET_FULL)).thenReturn(dc_oy);
		when(mockedDcTblDao.findAllDcs(DcTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_jq,dc_oy));
		
		when(mockedClusterTblDao.findClusterByClusterName("cluster1", ClusterTblEntity.READSET_FULL)).thenReturn(cluster_1);
		when(mockedClusterTblDao.findClusterByClusterName("cluster2", ClusterTblEntity.READSET_FULL)).thenReturn(cluster_2);
		when(mockedClusterTblDao.findByPK(1, ClusterTblEntity.READSET_FULL)).thenReturn(cluster_1);
		when(mockedClusterTblDao.findByPK(2, ClusterTblEntity.READSET_FULL)).thenReturn(cluster_2);
		when(mockedClusterTblDao.findAllClusters(ClusterTblEntity.READSET_FULL)).thenReturn(Arrays.asList(cluster_1,cluster_2));
		
		when(mockedShardTblDao.findShardByShardName("shard1", ShardTblEntity.READSET_FULL)).thenReturn(shard_1);
		when(mockedShardTblDao.findShardByShardName("shard2", ShardTblEntity.READSET_FULL)).thenReturn(shard_2);
		when(mockedShardTblDao.findByPK(1, ShardTblEntity.READSET_FULL)).thenReturn(shard_1);
		when(mockedShardTblDao.findByPK(2, ShardTblEntity.READSET_FULL)).thenReturn(shard_2);
		when(mockedShardTblDao.findAllByClusterId(1, ShardTblEntity.READSET_FULL)).thenReturn(Arrays.asList(shard_1,shard_2));
		
		when(mockedDcClusterTblDao.findAllByClusterId(1, DcClusterTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_1,dc_cluster_2));
		when(mockedDcClusterTblDao.findAllByDcId(1, DcClusterTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_1));
		when(mockedDcClusterTblDao.findAllByDcId(2, DcClusterTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_2));
		when(mockedDcClusterTblDao.findAllByMetaserverId(1, DcClusterTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_1));
		when(mockedDcClusterTblDao.findAllByMetaserverId(3, DcClusterTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_2));
		when(mockedDcClusterTblDao.findByPK(1, DcClusterTblEntity.READSET_FULL)).thenReturn(dc_cluster_1);
		when(mockedDcClusterTblDao.findByPK(2, DcClusterTblEntity.READSET_FULL)).thenReturn(dc_cluster_2);
		when(mockedDcClusterTblDao.findDcCluster(1, 1, DcClusterTblEntity.READSET_FULL)).thenReturn(dc_cluster_1);
		when(mockedDcClusterTblDao.findDcCluster(2, 1, DcClusterTblEntity.READSET_FULL)).thenReturn(dc_cluster_2);
		
		when(mockedDcClusterShardTblDao.findAllByDcClusterId(1, DcClusterShardTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_shard_1));
		when(mockedDcClusterShardTblDao.findAllByDcClusterId(2, DcClusterShardTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_shard_2));
		when(mockedDcClusterShardTblDao.findAllBySetinelId(1,DcClusterShardTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_shard_1));
		when(mockedDcClusterShardTblDao.findAllBySetinelId(2,DcClusterShardTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_shard_2));
		when(mockedDcClusterShardTblDao.findAllByShardId(1, DcClusterShardTblEntity.READSET_FULL)).thenReturn(Arrays.asList(dc_cluster_shard_1,dc_cluster_shard_2));
		when(mockedDcClusterShardTblDao.findByPK(1, DcClusterShardTblEntity.READSET_FULL)).thenReturn(dc_cluster_shard_1);
		when(mockedDcClusterShardTblDao.findByPK(2, DcClusterShardTblEntity.READSET_FULL)).thenReturn(dc_cluster_shard_2);
		when(mockedDcClusterShardTblDao.findDcClusterShard(1, 1, DcClusterShardTblEntity.READSET_FULL)).thenReturn(dc_cluster_shard_1);
		when(mockedDcClusterShardTblDao.findDcClusterShard(1, 2, DcClusterShardTblEntity.READSET_FULL)).thenReturn(dc_cluster_shard_2);
		
		when(mockedRedisTblDao.findAllByDcClusterShardId(1, RedisTblEntity.READSET_FULL)).thenReturn(Arrays.asList(redis_1,redis_2,redis_3,redis_4));
		when(mockedRedisTblDao.findByPK(1, RedisTblEntity.READSET_FULL)).thenReturn(redis_1);
		when(mockedRedisTblDao.findByPK(2, RedisTblEntity.READSET_FULL)).thenReturn(redis_2);
		when(mockedRedisTblDao.findByPK(3, RedisTblEntity.READSET_FULL)).thenReturn(redis_3);
		when(mockedRedisTblDao.findByPK(4, RedisTblEntity.READSET_FULL)).thenReturn(redis_4);
		
		when(mockedMetaserverTblDao.findAllByDcId(1, MetaserverTblEntity.READSET_FULL)).thenReturn(Arrays.asList(meta_1,meta_2));
		when(mockedMetaserverTblDao.findAllByDcId(2, MetaserverTblEntity.READSET_FULL)).thenReturn(Arrays.asList(meta_3));
		when(mockedMetaserverTblDao.findByPK(1, MetaserverTblEntity.READSET_FULL)).thenReturn(meta_1);
		when(mockedMetaserverTblDao.findByPK(2, MetaserverTblEntity.READSET_FULL)).thenReturn(meta_2);
		when(mockedMetaserverTblDao.findByPK(3, MetaserverTblEntity.READSET_FULL)).thenReturn(meta_3);
		when(mockedMetaserverTblDao.findMasterMetaserverByDcId(1, MetaserverTblEntity.READSET_FULL)).thenReturn(meta_1);
		when(mockedMetaserverTblDao.findMasterMetaserverByDcId(2, MetaserverTblEntity.READSET_FULL)).thenReturn(meta_3);
		
		when(mockedKeepercontainerTblDao.findAllByDcId(1, KeepercontainerTblEntity.READSET_FULL)).thenReturn(Arrays.asList(keepercontainer_1,keepercontainer_2));
		when(mockedKeepercontainerTblDao.findByPK(1, KeepercontainerTblEntity.READSET_FULL)).thenReturn(keepercontainer_1);
		when(mockedKeepercontainerTblDao.findByPK(2, KeepercontainerTblEntity.READSET_FULL)).thenReturn(keepercontainer_2);
		
		when(mockedSetinelTblDao.findAllByDcId(1, SetinelTblEntity.READSET_FULL)).thenReturn(Arrays.asList(setinel_1,setinel_2));
		when(mockedSetinelTblDao.findByPK(1, SetinelTblEntity.READSET_FULL)).thenReturn(setinel_1);
		when(mockedSetinelTblDao.findByPK(2, SetinelTblEntity.READSET_FULL)).thenReturn(setinel_2);
	}

	@Override
	protected String getXpipeMetaConfigFile() {
		return "metainfo.xml";
	}
}
