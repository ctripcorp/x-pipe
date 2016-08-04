package com.ctrip.xpipe.redis.console.service;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.Readset;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO;
import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO.DC;
import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO.Redis;
import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO.RedisRole;
import com.ctrip.xpipe.redis.console.entity.vo.ClusterVO.Shard;
import com.ctrip.xpipe.redis.console.web.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.web.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.ClusterTblEntity;
import com.ctrip.xpipe.redis.console.web.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.web.model.DcClusterShardTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcClusterShardTblEntity;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcClusterTblEntity;
import com.ctrip.xpipe.redis.console.web.model.DcTbl;
import com.ctrip.xpipe.redis.console.web.model.DcTblDao;
import com.ctrip.xpipe.redis.console.web.model.DcTblEntity;
import com.ctrip.xpipe.redis.console.web.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.web.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.web.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.web.model.MetaserverTblEntity;
import com.ctrip.xpipe.redis.console.web.model.RedisTbl;
import com.ctrip.xpipe.redis.console.web.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.web.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.web.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.web.model.ShardTbl;
import com.ctrip.xpipe.redis.console.web.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.web.model.ShardTblEntity;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author shyin
 *
 * Aug 3, 2016
 */
@Service
public class MetaInfoService {
	
	private DcTblDao dcTblDao;
	private ClusterTblDao clusterTblDao;
	private DcClusterTblDao dcClusterTblDao;
	private ShardTblDao shardTblDao;
	private DcClusterShardTblDao dcClusterShardTblDao;
	private RedisTblDao redisTblDao;
	private SetinelTblDao setinelTblDao;
	private MetaserverTblDao metaserverTblDao;
	private KeepercontainerTblDao keepercontainerTblDao;
	
	@PostConstruct
	private void postConstruct() {
		try {
			dcTblDao = ContainerLoader.getDefaultContainer().lookup(DcTblDao.class);
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
			redisTblDao = ContainerLoader.getDefaultContainer().lookup(RedisTblDao.class);
			setinelTblDao = ContainerLoader.getDefaultContainer().lookup(SetinelTblDao.class);
			metaserverTblDao = ContainerLoader.getDefaultContainer().lookup(MetaserverTblDao.class);
			keepercontainerTblDao = ContainerLoader.getDefaultContainer().lookup(KeepercontainerTblDao.class);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * @return list of all dc ids
	 * @throws DalException
	 */
	public List<String> getAllDcIds() throws DalException {
		List<String> dcIds = new ArrayList<String>(5);
		
		for(DcTbl dc : dcTblDao.findAllDcs(new Readset<DcTbl>(DcTblEntity.DC_ID)) ) {
			dcIds.add(dc.getDcId());
		}
			
		return dcIds;
	}
	
	/**
	 * @return list of all cluster ids
	 * @throws DalException
	 */
	public List<String> getAllClusterIds() throws DalException {
		List<String> clusterIds = new ArrayList<String>(10);
		
		for(ClusterTbl cluster : clusterTblDao.findAllClusters(new Readset<ClusterTbl>(ClusterTblEntity.CLUSTER_ID))) {
			clusterIds.add(cluster.getClusterId());
		}
		
		return clusterIds;
	}
	
	/**
	 * @param clusterId
	 * @return list of all shard ids inside a certain cluster
	 * @throws DalException
	 */
	public List<String> getAllClusterShardIds(String clusterId) throws DalException {
		List<String> clusterShardIds = new ArrayList<String>(20);
		
		for(ShardTbl shard : shardTblDao.findAllShardsByClusterid(clusterId, new Readset<ShardTbl>(ShardTblEntity.SHARD_ID))) {
			clusterShardIds.add(shard.getShardId());
		}
		
		return clusterShardIds;
	}
	
	
	/**
	 * @param dcId
	 * @param clusterId
	 * @param shardId
	 * @return
	 */
	public ShardMeta getDcClusterShardMeta(String dcId, String clusterId, String shardId) {
		ShardMeta shardMeta = new ShardMeta();
		
		/** dc-cluster-shard base info **/
		shardMeta.setId(shardId);
		// setinel id
		
		/** redis info **/
		
		return null;
	}
	
	/**
	 * @param dcId
	 * @param clusterId
	 * @return
	 */
	public ClusterMeta getDcClusterMeta(String dcId, String clusterId) {
		
		return null;
	}
	
	/**
	 * @param dcId
	 * @return all meta info inside a certain dc
	 * @throws DalException 
	 */
	public DcMeta getDcMeta(String dcId) throws DalException {
		DcMeta dcMeta = new DcMeta();
		
		/** Dc base info **/
		dcMeta.setId(dcId);
		
		/** Metaserver Info **/
		List<MetaserverTbl> metaservers = metaserverTblDao.findAllMetaserver(dcId, MetaserverTblEntity.READSET_FULL);
		for(MetaserverTbl metaserver : metaservers) {
			
			MetaServerMeta metaserverInfo = new MetaServerMeta();
			metaserverInfo.setIp(metaserver.getMetaserverIp());
			metaserverInfo.setPort(metaserver.getMetaserverPort());
			metaserverInfo.setParent(dcMeta);
			if (metaserver.getMetaserverRole().equals("master"))
				 metaserverInfo.setMaster(true); 
			else
				 metaserverInfo.setMaster(false);
			
			dcMeta.getMetaServers().add(metaserverInfo);
		}
		
		/** KeeperContainer Info **/
		
		/** Setinel Info **/
		
		/** Cluster Info **/
		
		return dcMeta;
	}
	
	/**
	 * @param clusterId
	 * @return cluster VO
	 * @throws DalException 
	 */
	public ClusterVO getClusterVO(String clusterId) throws DalException {
		
		/** ClusterVO Components **/
		ClusterVO clusterVO = new ClusterVO();
		List<DC> dcs = new ArrayList<DC>(5);
		
		/** Cluster base info **/
		clusterVO.setBaseInfo(clusterTblDao.findClusterByClusterid(clusterId, ClusterTblEntity.READSET_FULL));
		
		/** DCS info **/
		List<DcClusterTbl> dcClusters = dcClusterTblDao.findAllDcClusterByCluster(clusterId, DcClusterTblEntity.READSET_FULL);
		for(DcClusterTbl dcCluster : dcClusters) {
			DC dc = new DC();
			/** DC base info **/
			dc.setBaseInfo(dcTblDao.findDcByDcid(dcCluster.getDcId(), DcTblEntity.READSET_FULL));
			
			/** Get Shards info **/
			List<Shard> shards = new ArrayList<Shard>(10);
			List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardTblDao.findAllDcClusterShardByDcclusterid(dcCluster.getDcClusterId(), 
					DcClusterShardTblEntity.READSET_FULL);
			for(DcClusterShardTbl dcClusterShardTbl : dcClusterShardTbls) {
				Shard shard = new Shard();
				/** Shard base info **/
				shard.setBaseInfo(shardTblDao.findShardByShardid(dcClusterShardTbl.getShardId(), ShardTblEntity.READSET_FULL));
				
				/** Get Redis info **/
				List<Redis> redises = new ArrayList<Redis>(20);
				List<RedisTbl> redisTbls = redisTblDao.findAllRedis(dcClusterShardTbl.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
				for(RedisTbl redisTbl : redisTbls) {
					Redis redis = new Redis();
					
					redis.setId(redisTbl.getRedisId());
					redis.setIp(redisTbl.getRedisIp());
					redis.setPort(redisTbl.getRedisPort());
					redis.setActive(redisTbl.isRedisActive());
					redis.setRole(RedisRole.valueOf(redisTbl.getRedisRole().toUpperCase()));
					
					redises.add(redis);
				}
				
				shard.setRedises(redises);
				shards.add(shard);
			}
			
			dc.setShards(shards);
			dcs.add(dc);
		}
		
		clusterVO.setDcs(dcs);
		return clusterVO;
		
	}
}