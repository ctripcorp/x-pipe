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
import com.ctrip.xpipe.redis.console.model.KeepercontainerTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTbl;
import com.ctrip.xpipe.redis.console.model.MetaserverTblDao;
import com.ctrip.xpipe.redis.console.model.MetaserverTblEntity;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.RedisTblDao;
import com.ctrip.xpipe.redis.console.model.RedisTblEntity;
import com.ctrip.xpipe.redis.console.model.SetinelTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.ShardTblDao;
import com.ctrip.xpipe.redis.console.model.ShardTblEntity;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author shyin
 *
 * Aug 3, 2016
 */
@Service
public class MetaInfoService {

	public static long REDIS_MASTER_NULL = 0L;

	
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
	 * @return list of all dc names
	 * @throws DalException
	 */
	public List<String> getAllDcIds() throws DalException {
		List<String> dcIds = new ArrayList<String>(5);

		for(DcTbl dc : dcTblDao.findAllDcs(new Readset<DcTbl>(DcTblEntity.DC_NAME)) ) {
			dcIds.add(dc.getDcName());

		}
			
		return dcIds;
	}
	
	/**
	 * @return list of all cluster names
	 * @throws DalException
	 */
	public List<String> getAllClusterIds() throws DalException {
		List<String> clusterIds = new ArrayList<String>(10);
		
		for(ClusterTbl cluster : clusterTblDao.findAllClusters(new Readset<ClusterTbl>(ClusterTblEntity.CLUSTER_NAME))) {
			clusterIds.add(cluster.getClusterName());
		}
		
		return clusterIds;
	}
	
	/**
	 * @param clusterId
	 * @return list of all shard names inside a certain cluster
	 * @throws DalException
	 */
	public List<String> getAllClusterShardIds(String clusterId) throws DalException {
		List<String> clusterShardIds = new ArrayList<String>(20);		
		
		for(ShardTbl shard : shardTblDao.findAllByClusterId(clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL).getId()
				, new Readset<ShardTbl>(ShardTblEntity.SHARD_NAME))) {
			clusterShardIds.add(shard.getShardName());
		}
		
		return clusterShardIds;
	}
	
	
	/**
	 * @param dcId
	 * @param clusterId
	 * @param shardId
	 * @return ShardMeta
	 * @throws DalException 
	 */
	public ShardMeta getDcClusterShardMeta(String dcId, String clusterId, String shardId) throws DalException {
		ShardMeta shardMeta = new ShardMeta();
		
		DcClusterShardTbl dcClusterShardTbl = dcClusterShardTblDao.findDcClusterShard(
				shardTblDao.findShardByShardName(shardId, ShardTblEntity.READSET_FULL).getId(),
				dcClusterTblDao.findDcCluster(
						dcTblDao.findDcByDcName(dcId,DcTblEntity.READSET_FULL).getId(), 
						clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL).getId(), 
						DcClusterTblEntity.READSET_FULL).getDcClusterId(),
				DcClusterShardTblEntity.READSET_FULL);
		
		/** dc-cluster-shard base info **/
		shardMeta.setId(shardId);
		shardMeta.setSetinelId((int) dcClusterShardTbl.getSetinelId());
		
		/** redis info **/
		List<RedisTbl> redisTbls = redisTblDao.findAllByDcClusterShardId(dcClusterShardTbl.getDcClusterShardId()
				,RedisTblEntity.READSET_FULL);
		for(RedisTbl redisTbl : redisTbls) {
			String redisRole = redisTbl.getRedisRole();
			if(redisRole.equals("keeper")) {
				/** Keeper **/
				KeeperMeta keeperMeta = new KeeperMeta();
				
				keeperMeta.setId(redisTbl.getRedisName());
				keeperMeta.setIp(redisTbl.getRedisIp());
				keeperMeta.setPort(redisTbl.getRedisPort());
				if(redisTbl.getRedisMaster() == REDIS_MASTER_NULL) {
					keeperMeta.setMaster(null);
				} else {
					keeperMeta.setMaster(redisTblDao.findByPK(redisTbl.getRedisMaster(), 
							RedisTblEntity.READSET_FULL).getRedisName());
				}
				keeperMeta.setActive(redisTbl.isKeeperActive());
				
				shardMeta.addKeeper(keeperMeta);
			} else {
				/** Redis **/
				RedisMeta redisMeta = new RedisMeta();
				
				redisMeta.setId(redisTbl.getRedisName());
				redisMeta.setIp(redisTbl.getRedisIp());
				redisMeta.setPort(redisTbl.getRedisPort());
				if(redisTbl.getRedisMaster() == REDIS_MASTER_NULL) {
					redisMeta.setMaster(null);
				} else {
					redisMeta.setMaster(redisTblDao.findByPK(redisTbl.getRedisMaster(), 
							RedisTblEntity.READSET_FULL).getRedisName());
				}
				
				shardMeta.addRedis(redisMeta);
			}
		}
		
		return shardMeta;

	}
	
	/**
	 * @param dcId
	 * @param clusterId
	 * @return
	 * @throws DalException 
	 */
	public ClusterMeta getDcClusterMeta(String dcId, String clusterId) throws DalException {
		ClusterMeta clusterMeta = new ClusterMeta();
		
		ClusterTbl clusterTbl = clusterTblDao.findClusterByClusterName(
				clusterId, 
				ClusterTblEntity.READSET_FULL);
		DcClusterTbl dcClusterTbl = dcClusterTblDao.findDcCluster(
				dcTblDao.findDcByDcName(dcId, DcTblEntity.READSET_FULL).getId(), 
				clusterTbl.getId(), 
				DcClusterTblEntity.READSET_FULL);
		
		clusterMeta.setId(clusterTbl.getClusterName());
		clusterMeta.setActiveDc(dcTblDao.findByPK(clusterTbl.getActivedcId(), DcTblEntity.READSET_FULL).getDcName());
		
		
		return clusterMeta;
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

		List<MetaserverTbl> metaservers = metaserverTblDao.findAllByDcId(dcTblDao.findDcByDcName(dcId, DcTblEntity.READSET_FULL).getId()
				, MetaserverTblEntity.READSET_FULL);
		for(MetaserverTbl metaserver : metaservers) {
			
			MetaServerMeta metaserverInfo = new MetaServerMeta();
			metaserverInfo.setIp(metaserver.getMetaserverIp());
			metaserverInfo.setPort(metaserver.getMetaserverPort());
			metaserverInfo.setParent(dcMeta);

			// metaserver info
			
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

		clusterVO.setBaseInfo(clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL));
		
		/** DCS info **/
		List<DcClusterTbl> dcClusters = dcClusterTblDao.findAllByClusterId(clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL).getId()
				, DcClusterTblEntity.READSET_FULL);
		for(DcClusterTbl dcCluster : dcClusters) {
			DC dc = new DC();
			/** DC base info **/
			dc.setBaseInfo(dcTblDao.findByPK(dcCluster.getDcId(), DcTblEntity.READSET_FULL));
			
			/** Get Shards info **/
			List<Shard> shards = new ArrayList<Shard>(10);
			List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardTblDao.findAllByDcClusterId(dcCluster.getDcClusterId(), 

					DcClusterShardTblEntity.READSET_FULL);
			for(DcClusterShardTbl dcClusterShardTbl : dcClusterShardTbls) {
				Shard shard = new Shard();
				/** Shard base info **/

				shard.setBaseInfo(shardTblDao.findByPK(dcClusterShardTbl.getShardId(), ShardTblEntity.READSET_FULL));
				
				/** Get Redis info **/
				List<Redis> redises = new ArrayList<Redis>(20);
				List<RedisTbl> redisTbls = redisTblDao.findAllByDcClusterShardId(dcClusterShardTbl.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
				for(RedisTbl redisTbl : redisTbls) {
					Redis redis = new Redis();
					
					redis.setId(redisTbl.getRedisName());
					redis.setIp(redisTbl.getRedisIp());
					redis.setPort(redisTbl.getRedisPort());
					redis.setActive(true);
					if(redisTbl.getRedisRole().equals("redis")) {
						if(redisTbl.getRedisMaster() == REDIS_MASTER_NULL) {
							redis.setRole(RedisRole.MASTER);
						} else {
							redis.setRole(RedisRole.SLAVE);
						}
					} else {
						redis.setRole(RedisRole.KEEPER);
					}
					
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