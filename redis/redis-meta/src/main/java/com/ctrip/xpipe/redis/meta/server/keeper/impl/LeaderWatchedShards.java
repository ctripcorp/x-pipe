package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * @author wenchao.meng
 *
 * Aug 30, 2016
 */
public class LeaderWatchedShards {
	
	private Map<Pair<String, String>, LeaderMeta>  leaderMetas = new ConcurrentHashMap<>();
	
	/**
	 * @param clusterId
	 * @param shardId
	 * @return true if not exist
	 */
	public boolean addIfNotExist(String clusterId, String shardId){
		
		Pair<String, String> key = new Pair<>(clusterId, shardId);
		if(leaderMetas.get(key) != null){
			return false;
		}
				
		synchronized (leaderMetas) {
			if(leaderMetas.get(key) != null){
				return false;
			}
			leaderMetas.put(key, new LeaderMeta());
			return true;
		}
	}
	
	public void setActiveKeeper(String clusterId, String shardId, KeeperMeta keeperMeta){
		
		Pair<String, String> key = new Pair<>(clusterId, shardId);
		LeaderMeta leaderMeta = getLeaderMeta(key);
		leaderMeta.setActiveMeta(keeperMeta);
		
	}
	
	private LeaderMeta getLeaderMeta(Pair<String, String> key) {
		
		LeaderMeta leaderMeta = leaderMetas.get(key);
		if(leaderMeta == null){
			throw new IllegalArgumentException(key.toString());
		}
		return leaderMeta;
	}

	public KeeperMeta getActiveKeeper(String clusterId, String shardId){
		return getLeaderMeta(new Pair<>(clusterId, shardId)).getActiveMeta();
		
	}

	public boolean hasClusterShard(String clusterId, String shardId){
		return leaderMetas.get(new Pair<>(clusterId, shardId)) != null;
	}
	
	public void remove(String clusterId, String shardId){
		synchronized (leaderMetas) {
			leaderMetas.remove(new Pair<>(clusterId, shardId));
		}
	}
	
	public void removeByClusterId(String clusterId){
		
		synchronized (leaderMetas) {
			
			Set<Pair<String, String>> toDelete = new HashSet<>();
			for(Pair<String, String> key : leaderMetas.keySet()){
				if(key.getKey().equals(clusterId)){
					toDelete.add(key);
				}
			}

			for(Pair<String, String> key : toDelete){
				leaderMetas.remove(key);
			}
		}
	}
	
	public class LeaderMeta{
		
		private KeeperMeta activeMeta;
		
		public void setActiveMeta(KeeperMeta activeMeta) {
			this.activeMeta = activeMeta;
		}
		
		public KeeperMeta getActiveMeta() {
			return activeMeta;
		}
		
	}
}
