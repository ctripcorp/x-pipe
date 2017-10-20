package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.redis.console.model.*;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 * Aug 17, 2016
 */
public class DcMetaQueryVO {
	private DcTbl currentDc;
	// Key : dc-id
	private Map<Long, DcTbl> allDcs;
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
	// Key : cluster_id
	private Map<Long, List<DcClusterTbl>> allDcClusterMap;
    // Key : pair<clsuter_name, shard_name>
	private Map<Pair<String, String>,DcClusterShardTbl> dcClusterShardMap;

	public DcMetaQueryVO(DcTbl dc) {
		currentDc = dc;
		allDcs = new HashMap<>();
		clusterInfo = new HashMap<>();
		redisInfo = new HashMap<>();
		shardMap = new HashMap<>();
		redisMap = new HashMap<>();
		dcClusterMap = new HashMap<>();
		dcClusterShardMap = new HashMap<>();
		allDcClusterMap = new HashMap<>();
	}

	public void setAllDcs(Map<Long, DcTbl> allDcs) {
		this.allDcs = allDcs;
	}
	
	public void setAllDcClusterMap(Map<Long, List<DcClusterTbl>> allDcCluster) {
		this.allDcClusterMap = allDcCluster;
	}

	public void addClusterInfo(ClusterTbl clusterTbl) {
		if(!clusterInfo.containsKey(clusterTbl.getClusterName())) {
			clusterInfo.put(clusterTbl.getClusterName(), clusterTbl);
		}
	}
	
	public void addRedisInfo(RedisTbl redisTbl) {
		if(!redisInfo.containsKey(redisTbl.getId())) {
			redisInfo.put(redisTbl.getId(), redisTbl);
		}
	}
	
	public void addShardMap(String clusterName, ShardTbl shardTbl) {
		if(!shardMap.containsKey(clusterName)) {
			shardMap.put(clusterName, new LinkedList<ShardTbl>());
			shardMap.get(clusterName).add(shardTbl);
		} else {
			shardMap.get(clusterName).add(shardTbl);
		}
	}
	
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
	
	public void addDcClusterMap(String clusterName, DcClusterTbl dcClusterTbl) {
		if(!dcClusterMap.containsKey(clusterName)){
			dcClusterMap.put(clusterName, dcClusterTbl);
		}
	}
	
	public void addDcClusterShardMap(String clusterName, String shardName, DcClusterShardTbl dcClsuterShardTbl) {
		if(!dcClusterShardMap.containsKey(Pair.of(clusterName, shardName))) {
			dcClusterShardMap.put(Pair.of(clusterName, shardName), dcClsuterShardTbl);
		}
	}

	public DcTbl getCurrentDc() {return currentDc;}

	public Map<Long, DcTbl> getAllDcs() {return allDcs;}

	public Map<String, ClusterTbl> getClusterInfo() {return clusterInfo;}

	public Map<Long, RedisTbl> getRedisInfo() {return redisInfo;}

	public Map<String, List<ShardTbl>> getShardMap() {return shardMap;}

	public Map<String, Map<String, List<RedisTbl>>> getRedisMap() {return redisMap;}

	public Map<String, DcClusterTbl> getDcClusterMap() {return dcClusterMap;}
	
	public Map<Long, List<DcClusterTbl>> getAllDcClusterMap() {return allDcClusterMap;}

	public Map<Pair<String, String>, DcClusterShardTbl> getDcClusterShardMap() {return dcClusterShardMap;}
	
}
