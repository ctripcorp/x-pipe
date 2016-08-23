package com.ctrip.xpipe.redis.console.service.vo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import org.apache.commons.lang3.tuple.Triple;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public class DcMetaQueryVO {
	private DcTbl currentDc;
	// Key : dc-id
	private Map<Long, DcTbl> allDcs;
	// Key : Triple<dcId,clusterId,shardId>
	private Map<Triple<Long,Long,Long>, RedisTbl> allActiveKeepers;
	// Key : cluser_name
	private Map<String, ClusterTbl> clusterInfo;
	// Key : redis_name
	private Map<Long, RedisTbl> redisInfo;
	// Key : cluster_name
	private Map<String, List<ShardTbl>> shardMap;
	// Key : cluster_name   sub-Key : shard_name
	private Map<String, Map<String, List<RedisTbl>>> redisMap;
    // Key : cluster_name
	private Map<String, DcClusterTbl> dcClusterMap;
    // Key : pair<clsuter_name, shard_name>
	private Map<Pair<String, String>,DcClusterShardTbl> dcClusterShardMap;
	
	/**
	 * Constructor
	 */
	public DcMetaQueryVO(DcTbl dc) {
		currentDc = dc;
		allDcs = new HashMap<>();
		allActiveKeepers = new HashMap<>();
		clusterInfo = new HashMap<>();
		redisInfo = new HashMap<>();
		shardMap = new HashMap<>();
		redisMap = new HashMap<>();
		dcClusterMap = new HashMap<>();
		dcClusterShardMap = new HashMap<>();
	}


	/**
	 * set all dcs
	 */
	public void setAllDcs(Map<Long, DcTbl> allDcs) {
		this.allDcs = allDcs;
	}

	/**
	 * set active keeper info
	 */
	public void setAllActiveKeepers(Map<Triple<Long,Long,Long>, RedisTbl> allActiveKeepers) {
		this.allActiveKeepers = allActiveKeepers;
	}

	/**
	 * Add cluster info
	 * @param clusterTbl
	 */
	public void addClusterInfo(ClusterTbl clusterTbl) {
		if(!clusterInfo.containsKey(clusterTbl.getClusterName())) {
			clusterInfo.put(clusterTbl.getClusterName(), clusterTbl);
		}
	}
	
	/**
	 * Add redis info
	 * @param redisTbl
	 */
	public void addRedisInfo(RedisTbl redisTbl) {
		if(!redisInfo.containsKey(redisTbl.getId())) {
			redisInfo.put(redisTbl.getId(), redisTbl);
		}
	}
	
	/**
	 * Add shard info to shard map
	 * @param clusterName
	 * @param shardTbl
	 */
	public void addShardMap(String clusterName, ShardTbl shardTbl) {
		if(!shardMap.containsKey(clusterName)) {
			shardMap.put(clusterName, new LinkedList<ShardTbl>());
			shardMap.get(clusterName).add(shardTbl);
		} else {
			shardMap.get(clusterName).add(shardTbl);
		}
	}
	
	/**
	 * Add redis info to redis map
	 * @param clusterName
	 * @param shardName
	 * @param redisTbl
	 */
	public void addRedisMap(String clusterName, String shardName, RedisTbl redisTbl) {
		if(!redisMap.containsKey(clusterName)) {
			redisMap.put(clusterName, new HashMap<String, List<RedisTbl>>());
		}
		if(!redisMap.get(clusterName).containsKey(shardName)) {
			redisMap.get(clusterName).put(shardName, new LinkedList<RedisTbl>());
			redisMap.get(clusterName).get(shardName).add(redisTbl);
		} else {
			redisMap.get(clusterName).get(shardName).add(redisTbl);
		}
	}
	
	/**
	 * Add dc-cluster info to map
	 * @param clusterName
	 * @param dcClusterTbl
	 */
	public void addDcClusterMap(String clusterName, DcClusterTbl dcClusterTbl) {
		if(!dcClusterMap.containsKey(clusterName)){
			dcClusterMap.put(clusterName, dcClusterTbl);
		}
	}
	
	/**
	 * Add dc-cluster-shard to map
	 * @param clusterName
	 * @param shardName
	 * @param dcClsuterShardTbl
	 */
	public void addDcClusterShardMap(String clusterName, String shardName, DcClusterShardTbl dcClsuterShardTbl) {
		if(!dcClusterShardMap.containsKey(Pair.of(clusterName, shardName))) {
			dcClusterShardMap.put(Pair.of(clusterName, shardName), dcClsuterShardTbl);
		}
	}

	/**
	 * @return current dc
	 */
	public DcTbl getCurrentDc() {
		return currentDc;
	}

	/**
	 * @return all dcs
     */
	public Map<Long, DcTbl> getAllDcs() {return allDcs;}

	/**
	 * @return all active keepers
	 */
	public Map<Triple<Long, Long, Long>, RedisTbl> getAllActiveKeepers() {return allActiveKeepers;}

	/**
	 * @return the clusterInfo
	 */
	public Map<String, ClusterTbl> getClusterInfo() {
		return clusterInfo;
	}

	/**
	 * @return the redisInfo
	 */
	public Map<Long, RedisTbl> getRedisInfo() {
		return redisInfo;
	}

	/**
	 * @return the shardMap
	 */
	public Map<String, List<ShardTbl>> getShardMap() {
		return shardMap;
	}

	/**
	 * @return the redisMap
	 */
	public Map<String, Map<String, List<RedisTbl>>> getRedisMap() {
		return redisMap;
	}

	/**
	 * @return the dcClusterMap
	 */
	public Map<String, DcClusterTbl> getDcClusterMap() {
		return dcClusterMap;
	}

	/**
	 * @return the dcClusterShardMap
	 */
	public Map<Pair<String, String>, DcClusterShardTbl> getDcClusterShardMap() {
		return dcClusterShardMap;
	}
	
}
