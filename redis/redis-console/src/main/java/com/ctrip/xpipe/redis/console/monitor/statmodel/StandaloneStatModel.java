package com.ctrip.xpipe.redis.console.monitor.statmodel;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StandaloneStatModel implements java.io.Serializable{
	private static final long serialVersionUID = 1L;
	
	private Map<String, StandaloneClusterStat> clusterStats = new LinkedHashMap<>();
	
	public StandaloneStatModel(){}
		
	public Map<String, StandaloneClusterStat> getClusterStats() {
		return clusterStats;
	}

	public void addClusterStats(StandaloneClusterStat standaloneClusterStat) {
		clusterStats.put(standaloneClusterStat.getClusterId(), standaloneClusterStat);
	}

	public static class StandaloneClusterStat {
		private String clusterId;
		private Map<String, StandaloneShardStat> shardStats = new LinkedHashMap<>();
		
		public String getClusterId() {
			return clusterId;
		}
		public void setClusterId(String clusterId) {
			this.clusterId = clusterId;
		}
		public Map<String, StandaloneShardStat> getShardStats() {
			return shardStats;
		}
		public void addShardStats(StandaloneShardStat standaloneShardStat) {
			shardStats.put(standaloneShardStat.getShardId(), standaloneShardStat);
		}
		
	}
	
	public static class StandaloneShardStat {
		private String shardId;
		private Map<Long, StandaloneRedisStat> redisStats = new LinkedHashMap<>();
		private Map<Long, StandaloneRedisStat> keeperStats = new LinkedHashMap<>();
		
		public String getShardId() {
			return shardId;
		}
		public void setShardId(String shardId) {
			this.shardId = shardId;
		}
		public Map<Long, StandaloneRedisStat> getRedisStats() {
			return redisStats;
		}
		public Map<Long, StandaloneRedisStat> getKeeperStats() {
			return keeperStats;
		}
		public void addRedisStats(StandaloneRedisStat standaloneRedisStat) {
			redisStats.put(standaloneRedisStat.getId(), standaloneRedisStat);
		}
		public void addKeeperStats(StandaloneRedisStat standaloneRedisStat) {
			keeperStats.put(standaloneRedisStat.getId(), standaloneRedisStat);
		}
		
		public StandaloneRedisStat getRedisMaster() {
			for(StandaloneRedisStat redis : redisStats.values()) {
				if(redis.isMaster()) {
					return redis;
				}
			}
			return null;
		}
		
		public List<StandaloneRedisStat> getRedisSlaves() {
			List<StandaloneRedisStat> result = new LinkedList<>();
			for(StandaloneRedisStat redis : redisStats.values()) {
				if(!redis.isMaster()) {
					result.add(redis);
				}
			}
			return result;
		}
		
	}
	
	public static class StandaloneRedisStat {
		private Long id;
		private String ip;
		private int port;
		private String role;
		private boolean master;
		
		public Long getId() {
			return id;
		}
		public void setId(Long id) {
			this.id = id;
		}
		public String getIp() {
			return ip;
		}
		public void setIp(String ip) {
			this.ip = ip;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
		public String getRole() {
			return role;
		}
		public void setRole(String role) {
			this.role = role;
		}
		public boolean isMaster() {
			return master;
		}
		public void setMaster(boolean master) {
			this.master = master;
		}
	}
	
}
