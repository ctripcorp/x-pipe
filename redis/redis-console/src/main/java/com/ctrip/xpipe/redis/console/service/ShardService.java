package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.entity.DcClusterEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ShardListModel;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import org.springframework.lang.Nullable;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ShardService {

	class RegionShardContext {
		private final ClusterTbl cluster;
		private final AzGroupClusterEntity targetAzGroupCluster;
		private final List<DcClusterEntity> dcClustersInAzGroup;

		public RegionShardContext(ClusterTbl cluster, AzGroupClusterEntity targetAzGroupCluster,
								  List<DcClusterEntity> dcClustersInAzGroup) {
			this.cluster = cluster;
			this.targetAzGroupCluster = targetAzGroupCluster;
			this.dcClustersInAzGroup = dcClustersInAzGroup;
		}

		public ClusterTbl getCluster() { return cluster; }
		public AzGroupClusterEntity getTargetAzGroupCluster() { return targetAzGroupCluster; }
		public List<DcClusterEntity> getDcClustersInAzGroup() { return dcClustersInAzGroup; }
	}

	RegionShardContext validateRegionCluster(String clusterName, String regionName);

	void createRegionShard(RegionShardContext context, String shardName, List<RedisCreateInfo> redisCreateInfos)
		throws DalException;

	ShardTbl find(long shardId);
	ShardTbl find(String clusterName, String shardName);
	List<ShardTbl> findAllByClusterName(String clusterName);
	List<ShardTbl> findAllShardNamesByClusterName(String clusterName);
	List<ShardListModel> findAllByShardName(String shardName);
	ShardListModel findByReplId(long replId);
	ShardTbl createShard(String clusterName, ShardTbl shard, Map<Long, SentinelGroupModel> sentinels);
    ShardTbl findOrCreateShardIfNotExist(String clusterName, ShardTbl shard, List<DcClusterTbl> dcClusterTbls, Map<Long, SentinelGroupModel> sentinels);
	void deleteShard(String clusterName, String shardName);
	void deleteShards(ClusterTbl cluster, List<String> shardNames);
	List<ShardListModel> findAllUnhealthy();
    List<ShardTbl> findAllShardByDcCluster(long dcId, long clusterId);
	void deleteShardSentinels(List<ShardTbl> shards, ClusterTbl clusterTbl);

	void validateRedisCreateInfo(List<RedisCreateInfo> redisCreateInfos);

	void validateRedisCreateInfo(List<RedisCreateInfo> redisCreateInfos, @Nullable List<DcClusterEntity> dcClustersInAzGroup);

	void addRedises(ClusterTbl clusterTbl, String shardName, List<RedisCreateInfo> redisCreateInfos) throws DalException, ResourceNotFoundException;

	void addKeepers(ClusterTbl clusterTbl, String shardName, List<RedisCreateInfo> redisCreateInfos) throws DalException, ResourceNotFoundException;

	int doAddKeepers(String dcId, String clusterName, String shardName, String keeperDcId) throws DalException, ResourceNotFoundException;

}
