package com.ctrip.xpipe.redis.meta.server.meta;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author wenchao.meng
 *
 * Sep 6, 2016
 */
public class CurrentMeta implements Releasable{
	
	@JsonIgnore
	private Logger logger = LoggerFactory.getLogger(CurrentMeta.class);
	
	private Map<String, CurrentClusterMeta> currentMetas = new ConcurrentHashMap<>();
	
	public Set<String> allClusters() {
		return new HashSet<>(currentMetas.keySet());
	}

	public boolean hasCluster(String clusterId){
		return currentMetas.get(clusterId) != null;
	}
	
	public boolean hasShard(String clusterId, String shardId){
		return getCurrentShardMeta(clusterId, shardId) != null;
	}
	
	public boolean watchIfNotWatched(String clusterId, String shardId){
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.watchIfNotWatched();
	}
	
	public void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviveKeepers, KeeperMeta activeKeeper){
		
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentShardMeta.setSurviveKeepers(surviveKeepers, activeKeeper);
		
	}

	public void addResource(String clusterId, String shardId, Releasable releasable) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentShardMeta.addResource(releasable);
		
	}

	public List<KeeperMeta> getSurviveKeepers(String clusterId, String shardId){
		
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.getSurviveKeepers();
	}

	public boolean setKeeperActive(String clusterId, String shardId, KeeperMeta keeperMeta){
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.setActiveKeeper(keeperMeta);
	}
	
	public KeeperMeta getKeeperActive(String clusterId, String shardId){
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.getActiveKeeper();
	}

	
	public void setKeeperMaster(String clusterId, String shardId, InetSocketAddress keeperMaster){
		
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentShardMeta.setKeeperMaster(keeperMaster);
	}
	
	public InetSocketAddress getKeeperMaster(String clusterId, String shardId){
		
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.getKeeperMaster();
	}

	private CurrentShardMeta getCurrentShardMetaOrThrowException(String clusterId, String shardId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMeta(clusterId, shardId);
		if(currentShardMeta == null){
			throw new IllegalArgumentException("can not find :" + clusterId + "," + shardId);
		}
		return currentShardMeta;
	}

	private CurrentShardMeta getCurrentShardMeta(String clusterId, String shardId) {
		CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterId);
		if( currentClusterMeta == null){
			return null;
		}
		CurrentShardMeta currentShardMeta = currentClusterMeta.getShard(shardId);
		return currentShardMeta;
	}

	@Override
	public void release() throws Exception {
		
		for(CurrentClusterMeta currentClusterMeta : currentMetas.values()){
			try{
				currentClusterMeta.release();
			}catch(Exception e){
				logger.error("[release]" + currentClusterMeta.getClusterId(), e);
			}
		}
	}

	public void addCluster(final ClusterMeta clusterMeta){
		CurrentClusterMeta currentClusterMeta = MapUtils.getOrCreate(currentMetas, clusterMeta.getId(), new ObjectFactory<CurrentClusterMeta>() {

			@Override
			public CurrentClusterMeta create() {
				return new CurrentClusterMeta(clusterMeta.getId());
			}
		});
		
		for(ShardMeta shardMeta : clusterMeta.getShards().values()){
			currentClusterMeta.addShard(shardMeta);
		}
	}
	
	public CurrentClusterMeta removeCluster(String clusterId){
		CurrentClusterMeta currentClusterMeta = currentMetas.remove(clusterId);
		try {
			currentClusterMeta.release();
		} catch (Exception e) {
			logger.error("[remove]" + clusterId, e);
		}
		return currentClusterMeta;
	}

	public void changeCluster(ClusterMetaComparator comparator){
		
		String clusterId = comparator.getCurrent().getId();
		final CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterId);
		if(currentClusterMeta == null){
			throw new IllegalArgumentException("cluster not exist:" + comparator);
		}
		
		comparator.accept(new MetaComparatorVisitor<ShardMeta>() {
			
			@Override
			public void visitRemoved(ShardMeta removed) {
				currentClusterMeta.removeShard(removed);;
			}
			
			@Override
			public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {
				currentClusterMeta.changeShard((ShardMetaComparator) comparator);;
			}
			
			@Override
			public void visitAdded(ShardMeta added) {
				currentClusterMeta.addShard(added);
			}
		});
	}

	
	public static class CurrentClusterMeta implements Releasable{
		
		@JsonIgnore
		private static Logger logger = LoggerFactory.getLogger(CurrentClusterMeta.class);
		
		private String clusterId;
		private Map<String, CurrentShardMeta> clusterMetas = new ConcurrentHashMap<>();
		
		public CurrentClusterMeta(){
			
		}
		public CurrentClusterMeta(String clusterId) {
			this.clusterId = clusterId;
		}

		public CurrentShardMeta getShard(String shardId) {
			return clusterMetas.get(shardId);
		}
		
		@Override
		public void release() throws Exception {
			
			logger.info("[release]{}", clusterId);
			for(CurrentShardMeta currentShardMeta : clusterMetas.values()){
				currentShardMeta.release();
			}
			
		}

		public void addShard(final ShardMeta shardMeta) {
			
			CurrentShardMeta currentShardMeta = MapUtils.getOrCreate(clusterMetas, shardMeta.getId(), new ObjectFactory<CurrentShardMeta>() {
				@Override
				public CurrentShardMeta create() {
					return new CurrentShardMeta(clusterId, shardMeta.getId());
				}
			});
			
			InetSocketAddress inetSocketAddress = getDefaultKeeperMaster(shardMeta);
			logger.info("[addShard][default keeper master]{}", inetSocketAddress);
			currentShardMeta.setKeeperMaster(inetSocketAddress);
		}
		
		private InetSocketAddress getDefaultKeeperMaster(ShardMeta shardMeta) {
			
			String upstream = shardMeta.getUpstream();
			try{
				if(!StringUtil.isEmpty(upstream)){
					return IpUtils.parseSingle(upstream);
				}
			}catch(Exception e){
				logger.error("[getDefaultKeeperMaster]", e);
			}

			RedisMeta redisMaster = null;
			for(RedisMeta redisMeta : shardMeta.getRedises()){
				if(redisMeta.isMaster()){
					redisMaster = redisMeta;
				}
			}			
			if(redisMaster != null){
				return new InetSocketAddress(redisMaster.getIp(), redisMaster.getPort());
			}
			
			
			if(shardMeta.getRedises().size() == 0){
				logger.warn("[getDefaultKeeperMaster][no redis]{}", shardMeta);
				return null;
			}
			
			RedisMeta redisMeta = shardMeta.getRedises().get(0);
			logger.warn("[getDefaultKeeperMaster][no redis master, no upstream, use first redis instead]{}, {}", clusterId, redisMaster);
			return new InetSocketAddress(redisMeta.getIp(), redisMeta.getPort());
		}
		
		public void removeShard(ShardMeta shardMeta) {
			
			CurrentShardMeta currentShardMeta = clusterMetas.remove(shardMeta.getId());
			try {
				currentShardMeta.release();
			} catch (Exception e) {
				logger.error("[removeShard]" + shardMeta.getId(), e);
			}
		}
		
		public void changeShard(ShardMetaComparator comparator) {
			CurrentShardMeta currentShardMeta = clusterMetas.get(comparator.getCurrent().getId());
			if(currentShardMeta == null){
				throw new IllegalArgumentException("unfound shard:" + comparator);
			}
			//nothing to do
		}

		public String getClusterId() {
			return clusterId;
		}

	}
	
	
	public static class CurrentShardMeta implements Releasable{
		
		@JsonIgnore
		private Logger logger = LoggerFactory.getLogger(getClass());
		
		@JsonIgnore
		private List<Releasable> resources = new LinkedList<>();

		private AtomicBoolean     watched = new AtomicBoolean(false);
		private String 			  clusterId, shardId;
		private List<KeeperMeta>  surviveKeepers = new LinkedList<>();
		private InetSocketAddress keeperMaster;
		
		public CurrentShardMeta(){
			
		}
		
		public void addResource(Releasable releasable) {
			resources.add(releasable);
		}

		@Override
		public void release() throws Exception {
			logger.info("[release]{},{}", clusterId, shardId);
			for(Releasable resource : resources){
				try{
					resource.release();
				}catch(Exception e){
					logger.error("[release]" + resource, e);
				}
			}
		}

		public boolean watchIfNotWatched(){
			return watched.compareAndSet(false, true);
		}

		@JsonIgnore
		public boolean setActiveKeeper(KeeperMeta activeKeeper) {
			
			if(!checkIn(surviveKeepers, activeKeeper)){
				throw new IllegalArgumentException("active not in all survivors " + activeKeeper + ", all:" + this.surviveKeepers);
			}
			return doSetActive(activeKeeper);
		}
		
		public CurrentShardMeta(String clusterId, String shardId) {
			this.clusterId = clusterId;
			this.shardId = shardId;
		}
		
		@JsonIgnore
		public KeeperMeta getActiveKeeper() {
			for(KeeperMeta survive : surviveKeepers){
				if(survive.isActive()){
					return survive;
				}
			}
			return null;
		}

		@SuppressWarnings("unchecked")
		public List<KeeperMeta> getSurviveKeepers() {
			return (List<KeeperMeta>) MetaClone.clone((Serializable)surviveKeepers);
		}

		@SuppressWarnings("unchecked")
		public void setSurviveKeepers(List<KeeperMeta> surviveKeepers, KeeperMeta activeKeeper) {

			if(surviveKeepers.size() > 0){
				if(!checkIn(surviveKeepers, activeKeeper)){
					throw new IllegalArgumentException("active not in all survivors " + activeKeeper + ", all:" + this.surviveKeepers);
				}
				this.surviveKeepers = (List<KeeperMeta>) MetaClone.clone((Serializable)surviveKeepers);
				logger.info("[setSurviveKeepers]{},{},{}, {}", clusterId, shardId, surviveKeepers, activeKeeper);
				doSetActive(activeKeeper);
			}else{
				logger.info("[setSurviveKeepers][survive keeper none, clear]{},{},{}, {}", clusterId, shardId, surviveKeepers, activeKeeper);
				this.surviveKeepers.clear();
			}
		}
		
		public boolean doSetActive(KeeperMeta activeKeeper) {
			
			boolean changed = false;
			logger.info("[doSetActive]{},{},{}", clusterId, shardId, activeKeeper);
			for(KeeperMeta survive : this.surviveKeepers){
				
				if(MetaUtils.same(survive, activeKeeper)){
					if(!survive.isActive()){
						survive.setActive(true);
						changed = true;
					}
				}else{
					if(survive.isActive()){
						survive.setActive(false);
					}
				}
			}
			return changed;
		}

		private boolean checkIn(List<KeeperMeta> surviveKeepers, KeeperMeta activeKeeper) {
			for(KeeperMeta survive : surviveKeepers){
				if(MetaUtils.same(survive, activeKeeper)){
					return true;
				}
			}
			return false;
		}

		public InetSocketAddress getKeeperMaster() {
			return keeperMaster;
		}

		public void setKeeperMaster(InetSocketAddress keeperMaster) {
			logger.info("[setKeeperMaster]{},{},{}", clusterId, shardId, keeperMaster);
			this.keeperMaster = keeperMaster;
		}
		
		public String getShardId() {
			return shardId;
		}

	}
	
	@Override
	public String toString() {
		JsonCodec codec = new JsonCodec(true, true);
		return codec.encode(this);
	}


	public static CurrentMeta fromJson(String json){
		JsonCodec jsonCodec = new JsonCodec(true, true);
		return jsonCodec.decode(json, CurrentMeta.class);
	}

}
