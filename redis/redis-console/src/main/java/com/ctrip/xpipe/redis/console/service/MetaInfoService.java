package com.ctrip.xpipe.redis.console.service;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
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
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.SetinelMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author shyin
 *
 *         Aug 3, 2016
 */
@Service
public class MetaInfoService {
	private static Logger logger = LoggerFactory.getLogger(MetaInfoService.class);
	
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
			logger.info("[MetaInfoService][Construct Dao]");
			
			dcTblDao = ContainerLoader.getDefaultContainer().lookup(DcTblDao.class);
			clusterTblDao = ContainerLoader.getDefaultContainer().lookup(ClusterTblDao.class);
			dcClusterTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterTblDao.class);
			shardTblDao = ContainerLoader.getDefaultContainer().lookup(ShardTblDao.class);
			dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
			redisTblDao = ContainerLoader.getDefaultContainer().lookup(RedisTblDao.class);
			setinelTblDao = ContainerLoader.getDefaultContainer().lookup(SetinelTblDao.class);
			metaserverTblDao = ContainerLoader.getDefaultContainer().lookup(MetaserverTblDao.class);
			keepercontainerTblDao = ContainerLoader.getDefaultContainer().lookup(KeepercontainerTblDao.class);

		} catch (Exception ex) {
			logger.error("[MetaInfoService],[Construct fail]",ex);
		}

	}

	/**
	 * @return list of all dc names
	 * @throws DalException
	 */
	public List<String> getAllDcIds() throws DalException{
		List<String> dcIds = new ArrayList<String>(5);

		for (DcTbl dc : dcTblDao.findAllDcs(DcTblEntity.READSET_FULL)) {
			dcIds.add(dc.getDcName());
		}

		return dcIds;
	}

	/**
	 * @return list of all cluster names
	 * @throws DalException
	 */
	public List<String> getAllClusterIds() throws DalException {
		List<String> clusterIds = new ArrayList<String>(20);

		for (ClusterTbl cluster : clusterTblDao
				.findAllClusters(ClusterTblEntity.READSET_FULL)) {
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

		for (ShardTbl shard : shardTblDao.findAllByClusterId(
				clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL).getId(),
				ShardTblEntity.READSET_FULL)) {
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

		/** Basic Tbl Info **/
		DcTbl dcInfo = dcTblDao.findDcByDcName(dcId, DcTblEntity.READSET_FULL);
		ClusterTbl clusterInfo = clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL);
		ShardTbl shardInfo = shardTblDao.findShardByShardName(shardId, ShardTblEntity.READSET_FULL);
		DcClusterTbl dcClusterInfo = dcClusterTblDao.findDcCluster(dcInfo.getId(), clusterInfo.getId(),
				DcClusterTblEntity.READSET_FULL);
		DcClusterShardTbl dcClusterShardInfo = dcClusterShardTblDao.findDcClusterShard(shardInfo.getId(),
				dcClusterInfo.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);

		/** dc-cluster-shard base info **/
		shardMeta.setId(shardInfo.getShardName());
		/** upstream **/
		if (clusterInfo.getActivedcId() == dcInfo.getId()) {
			shardMeta.setUpstream("");
		} else {
			/** find active keeper in active dc **/
			DcClusterTbl activeDcClusterInfo = dcClusterTblDao.findDcCluster(clusterInfo.getActivedcId(),
					clusterInfo.getId(), DcClusterTblEntity.READSET_FULL);
			DcClusterShardTbl activeDcClusterShardInfo = dcClusterShardTblDao.findDcClusterShard(shardInfo.getId(),
					activeDcClusterInfo.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);
			
			List<RedisTbl> activeDcClusterShardRedises = redisTblDao.findAllByDcClusterShardId(
					activeDcClusterShardInfo.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
			for (RedisTbl activeDcClusterShardRedis : activeDcClusterShardRedises) {
				if (activeDcClusterShardRedis.getRedisRole().equals("keeper")
						&& activeDcClusterShardRedis.isKeeperActive() == true) {

					StringBuilder sb = new StringBuilder(30);
					sb.append(activeDcClusterShardRedis.getRedisIp());
					sb.append(":");
					sb.append(String.valueOf(activeDcClusterShardRedis.getRedisPort()));

					shardMeta.setUpstream(sb.toString());
					break;
				}
			}
		}
		shardMeta.setSetinelId(dcClusterShardInfo.getSetinelId());
		shardMeta.setSetinelMonitorName(shardInfo.getSetinelMonitorName());
		shardMeta.setPhase(dcClusterShardInfo.getDcClusterShardPhase());

		/** redis info **/
		List<RedisTbl> redisInfos = redisTblDao.findAllByDcClusterShardId(dcClusterShardInfo.getDcClusterShardId(),
				RedisTblEntity.READSET_FULL);
		for (RedisTbl redisInfo : redisInfos) {
			String redisRole = redisInfo.getRedisRole();
			if (redisRole.equals("keeper")) {
				/** Keeper **/
				KeeperMeta keeperMeta = new KeeperMeta();

				keeperMeta.setId(redisInfo.getRedisName());
				keeperMeta.setIp(redisInfo.getRedisIp());
				keeperMeta.setPort(redisInfo.getRedisPort());
				if (redisInfo.getRedisMaster() == REDIS_MASTER_NULL) {
					keeperMeta.setMaster("");
				} else {
					StringBuilder sb = new StringBuilder(30);
					
					RedisTbl redisMaster = redisTblDao.findByPK(redisInfo.getRedisMaster(), RedisTblEntity.READSET_FULL);
					sb.append(redisMaster.getRedisIp());
					sb.append(":");
					sb.append(String.valueOf(redisMaster.getRedisPort()));

					keeperMeta.setMaster(sb.toString());
				}
				keeperMeta.setActive(redisInfo.isKeeperActive());
				keeperMeta.setKeeperContainerId(redisInfo.getKeepercontainerId());
				keeperMeta.setParent(shardMeta);

				shardMeta.addKeeper(keeperMeta);
			} else {
				/** Redis **/
				RedisMeta redisMeta = new RedisMeta();

				redisMeta.setId(redisInfo.getRedisName());
				redisMeta.setIp(redisInfo.getRedisIp());
				redisMeta.setPort(redisInfo.getRedisPort());
				if (redisInfo.getRedisMaster() == REDIS_MASTER_NULL) {
					redisMeta.setMaster("");
				} else {
					StringBuilder sb = new StringBuilder(30);
					
					RedisTbl redisMaster = redisTblDao.findByPK(redisInfo.getRedisMaster(), RedisTblEntity.READSET_FULL);
					sb.append(redisMaster.getRedisIp());
					sb.append(":");
					sb.append(String.valueOf(redisMaster.getRedisPort()));

					redisMeta.setMaster(sb.toString());
				}
				redisMeta.setParent(shardMeta);

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

		DcTbl dcInfo = dcTblDao.findDcByDcName(dcId, DcTblEntity.READSET_FULL);
		ClusterTbl clusterInfo = clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL);
		DcClusterTbl dcClusterInfo = dcClusterTblDao.findDcCluster(dcInfo.getId(), clusterInfo.getId(),
				DcClusterTblEntity.READSET_FULL);

		clusterMeta.setId(clusterInfo.getClusterName());
		clusterMeta.setActiveDc(dcTblDao.findByPK(clusterInfo.getActivedcId(), DcTblEntity.READSET_FULL).getDcName());
		clusterMeta.setPhase(dcClusterInfo.getDcClusterPhase());
		clusterMeta.setLastModifiedTime(clusterInfo.getClusterLastModifiedTime());

		/** Shards **/
		List<DcClusterShardTbl> dcClusterShards = dcClusterShardTblDao
				.findAllByDcClusterId(dcClusterInfo.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);
		for (DcClusterShardTbl dcClusterShard : dcClusterShards) {
			ShardTbl shardInfo = shardTblDao.findByPK(dcClusterShard.getShardId(), ShardTblEntity.READSET_FULL);
			
			ShardMeta shardMeta = getDcClusterShardMeta(
					dcInfo.getDcName(), 
					clusterInfo.getClusterName(),
					shardInfo.getShardName());

			clusterMeta.addShard(shardMeta);
		}

		return clusterMeta;
	}

	/**
	 * @param dcId
	 * @return all meta info inside a certain dc
	 * @throws DalException
	 */
	public DcMeta getDcMeta(String dcId) throws DalException {
		DcMeta dcMeta = new DcMeta();

		DcTbl dcInfo = dcTblDao.findDcByDcName(dcId, DcTblEntity.READSET_FULL);
		
		/** Dc base info **/
		dcMeta.setId(dcInfo.getDcName());
		dcMeta.setLastModifiedTime(dcInfo.getDcLastModifiedTime());

		/** Metaserver Info **/
		List<MetaserverTbl> metaservers = metaserverTblDao.findAllByDcId(
				dcInfo.getId(), MetaserverTblEntity.READSET_FULL);
		for (MetaserverTbl metaserver : metaservers) {
			MetaServerMeta metaserverInfo = new MetaServerMeta();
			
			metaserverInfo.setIp(metaserver.getMetaserverIp());
			metaserverInfo.setPort(metaserver.getMetaserverPort());
			if (metaserver.getMetaserverRole().equals("master")) {
				metaserverInfo.setMaster(true);
			} else {
				metaserverInfo.setMaster(false);
			}
			metaserverInfo.setParent(dcMeta);

			dcMeta.getMetaServers().add(metaserverInfo);
		}

		/** KeeperContainer Info **/
		List<KeepercontainerTbl> keepercontainers = keepercontainerTblDao.findAllByDcId(
				dcInfo.getId(), KeepercontainerTblEntity.READSET_FULL);
		for (KeepercontainerTbl keepercontainer : keepercontainers) {
			KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
			
			keeperContainerMeta.setId(keepercontainer.getKeepercontainerId());
			keeperContainerMeta.setIp(keepercontainer.getKeepercontainerIp());
			keeperContainerMeta.setPort(keepercontainer.getKeepercontainerPort());
			keeperContainerMeta.setParent(dcMeta);

			dcMeta.addKeeperContainer(keeperContainerMeta);
		}

		/** Setinel Info **/
		List<SetinelTbl> setinels = setinelTblDao.findAllByDcId(
				dcInfo.getId(), SetinelTblEntity.READSET_FULL);
		for (SetinelTbl setinel : setinels) {
			SetinelMeta setinelMeta = new SetinelMeta();
			
			setinelMeta.setId(setinel.getSetinelId());
			setinelMeta.setAddress(setinel.getSetinelAddress());
			setinelMeta.setParent(dcMeta);

			dcMeta.addSetinel(setinelMeta);
		}

		/** Cluster Info **/
		List<DcClusterTbl> dcClusters = dcClusterTblDao.findAllByDcId(
				dcInfo.getId(), DcClusterTblEntity.READSET_FULL);
		for (DcClusterTbl dcCluster : dcClusters) {
			ClusterTbl clusterInfo = clusterTblDao.findByPK(dcCluster.getClusterId(), ClusterTblEntity.READSET_FULL);
			
			ClusterMeta clusterMeta = getDcClusterMeta(dcInfo.getDcName(), clusterInfo.getClusterName());

			dcMeta.addCluster(clusterMeta);
		}

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
		ClusterTbl clusterInfo = clusterTblDao.findClusterByClusterName(clusterId, ClusterTblEntity.READSET_FULL);
		clusterVO.setBaseInfo(clusterInfo);

		/** DCS info **/
		List<DcClusterTbl> dcClusters = dcClusterTblDao.findAllByClusterId(
				clusterInfo.getId(),
				DcClusterTblEntity.READSET_FULL);
		for (DcClusterTbl dcCluster : dcClusters) {
			DC dc = new DC();
			
			/** DC base info **/
			DcTbl dcInfo = dcTblDao.findByPK(dcCluster.getDcId(), DcTblEntity.READSET_FULL);
			dc.setBaseInfo(dcInfo);

			/** Get Shards info **/
			List<Shard> shards = new ArrayList<Shard>(10);
			List<DcClusterShardTbl> dcClusterShards = dcClusterShardTblDao
					.findAllByDcClusterId(dcCluster.getDcClusterId(), DcClusterShardTblEntity.READSET_FULL);
			for (DcClusterShardTbl dcClusterShard : dcClusterShards) {
				Shard shard = new Shard();
				
				/** Shard base info **/
				ShardTbl shardInfo = shardTblDao.findByPK(dcClusterShard.getShardId(), ShardTblEntity.READSET_FULL);
				shard.setBaseInfo(shardInfo);

				/** Get Redis info **/
				List<Redis> redises = new ArrayList<Redis>(20);
				List<RedisTbl> redisInfos = redisTblDao.findAllByDcClusterShardId(
						dcClusterShard.getDcClusterShardId(), RedisTblEntity.READSET_FULL);
				for (RedisTbl redisInfo : redisInfos) {
					Redis redis = new Redis();

					redis.setId(redisInfo.getRedisName());
					redis.setIp(redisInfo.getRedisIp());
					redis.setPort(redisInfo.getRedisPort());
					redis.setActive(true);
					if (redisInfo.getRedisRole().equals("redis")) {
						if (redisInfo.getRedisMaster() == REDIS_MASTER_NULL) {
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
