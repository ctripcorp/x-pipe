package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.meta.server.meta.impl.CurrentCRDTShardMeta;
import com.ctrip.xpipe.redis.meta.server.meta.impl.CurrentKeeperShardMeta;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wenchao.meng
 *
 *         Sep 6, 2016
 */
public class CurrentMeta implements Releasable {

	@JsonIgnore
	private Logger logger = LoggerFactory.getLogger(CurrentMeta.class);

	private Map<Long, CurrentClusterMeta> currentMetas = new ConcurrentHashMap<>();

	private static final String CLUSTER_NOT_SUPPORT_KEEPER_TEMPLATE = "cluster: %d, type: %s not support keeper";

	private static final String CLUSTER_NOT_SUPPORT_PEER_MASTER_TEMPLATE = "cluster: %d, type: %s not support peer master";

	public Set<Long> allClusters() {
		return new HashSet<>(currentMetas.keySet());
	}

	public Set<CurrentClusterMeta> allClusterMetas() {
		return new HashSet<>(currentMetas.values());
	}

	public void updateClusterName(Long clusterDbId, String clusterId) {
		CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterDbId);
		if (null == currentClusterMeta) {
			logger.warn("[updateClusterName] unfound cluster {}", clusterDbId);
			return;
		}
		currentClusterMeta.setClusterId(clusterId);
	}

	public boolean hasCluster(Long clusterDbId) {
		return currentMetas.get(clusterDbId) != null;
	}

	public boolean hasShard(Long clusterDbId, Long shardDbId) {
		return getCurrentShardMeta(clusterDbId, shardDbId) != null;
	}

	public boolean watchIfNotWatched(Long clusterDbId, Long shardDbId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.watchIfNotWatched();
	}

	public void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviveKeepers,
			KeeperMeta activeKeeper) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		currentShardMeta.setSurviveKeepers(surviveKeepers, activeKeeper);

	}

	public void addResource(Long clusterDbId, Long shardDbId, Releasable releasable) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		currentShardMeta.addResource(releasable);

	}

	public List<KeeperMeta> getSurviveKeepers(Long clusterDbId, Long shardDbId) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getSurviveKeepers();
	}

	public boolean setKeeperActive(Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.setActiveKeeper(keeperMeta);
	}

	public KeeperMeta getKeeperActive(Long clusterDbId, Long shardDbId) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getActiveKeeper();
	}

	public boolean setKeeperMaster(Long clusterDbId, Long shardDbId, Pair<String, Integer> keeperMaster) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.setKeeperMaster(keeperMaster);
	}

	public Pair<String, Integer> getKeeperMaster(Long clusterDbId, Long shardDbId) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getKeeperMaster();
	}

	public void setCurrentCRDTMaster(Long clusterDbId, Long shardDbId,  RedisMeta peerMaster) {
		checkClusterSupportPeerMaster(clusterDbId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		currentCRDTShardMeta.setCurrentMaster(peerMaster);
	}

	public RedisMeta getCurrentCRDTMaster(Long clusterDbId, Long shardDbId) {
		checkClusterSupportPeerMaster(clusterDbId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentCRDTShardMeta.getCurrentMaster();
	}

	public RedisMeta getCurrentMaster(Long clusterDbId, Long shardDbId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		if (currentShardMeta instanceof CurrentCRDTShardMeta) {
			return ((CurrentCRDTShardMeta) currentShardMeta).getCurrentMaster();
		} else if (currentShardMeta instanceof CurrentKeeperShardMeta) {
			Pair<String, Integer> master = ((CurrentKeeperShardMeta) currentShardMeta).getKeeperMaster();
			if (null == master) return null;
			return new RedisMeta().setIp(master.getKey()).setPort(master.getValue());
		}

		return null;
	}

	public void setPeerMaster(String dcId, Long clusterDbId, Long shardDbId, RedisMeta peerMaster) {
		checkClusterSupportPeerMaster(clusterDbId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		currentCRDTShardMeta.setPeerMaster(dcId, peerMaster);
	}

	public RedisMeta getPeerMaster(String dcId, Long clusterDbId, Long shardDbId) {
		checkClusterSupportPeerMaster(clusterDbId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentCRDTShardMeta.getPeerMaster(dcId);
	}

	public RouteMeta getClusterRouteByDcId(Long clusterDbId, String dcId) {
		CurrentClusterMeta clusterMeta = currentMetas.get(clusterDbId);
		return clusterMeta.getRouteByDcId(dcId);
	}

	public List<String> updateClusterRoutes(ClusterMeta clusterMeta, Map<String, RouteMeta> routes) {
		CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterMeta.getDbId());
		return currentClusterMeta.updateRoutes(routes, clusterMeta);
	}

	public void removePeerMaster(String dcId, Long clusterDbId, Long shardDbId) {
		checkClusterSupportPeerMaster(clusterDbId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		currentCRDTShardMeta.removePeerMaster(dcId);
	}

	public Set<String> getUpstreamPeerDcs(Long clusterDbId, Long shardDbId) {
		checkClusterSupportPeerMaster(clusterDbId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentCRDTShardMeta.getUpstreamPeerDcs();
	}

	public Map<String, RedisMeta> getAllPeerMasters(Long clusterDbId, Long shardDbId) {
		checkClusterSupportPeerMaster(clusterDbId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentCRDTShardMeta.getAllPeerMasters();
	}

	private void checkClusterSupportKeeper(Long clusterDbId) {
		if (!currentMetas.containsKey(clusterDbId)) return;

		String clusterType = currentMetas.get(clusterDbId).clusterType;
		if (!ClusterType.lookup(clusterType).supportKeeper()) {
			throw new IllegalArgumentException(String.format(CLUSTER_NOT_SUPPORT_KEEPER_TEMPLATE, clusterDbId, clusterType));
		}
	}

	private void checkClusterSupportPeerMaster(Long clusterDbId) {
		if (!currentMetas.containsKey(clusterDbId)) return;

		String clusterType = currentMetas.get(clusterDbId).clusterType;
		if (!ClusterType.lookup(clusterType).supportMultiActiveDC()) {
			throw new IllegalArgumentException(String.format(CLUSTER_NOT_SUPPORT_PEER_MASTER_TEMPLATE, clusterDbId, clusterType));
		}
	}

	private CurrentShardMeta getCurrentShardMetaOrThrowException(Long clusterDbId, Long shardDbId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMeta(clusterDbId, shardDbId);
		if (currentShardMeta == null) {
			throw new IllegalArgumentException("can not find :" + clusterDbId + "," + shardDbId);
		}
		return currentShardMeta;
	}

	private CurrentShardMeta getCurrentShardMeta(Long clusterDbId, Long shardDbId) {
		CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterDbId);
		if (currentClusterMeta == null) {
			return null;
		}
		return currentClusterMeta.getShard(shardDbId);
	}

	@Override
	public void release() throws Exception {

		for (CurrentClusterMeta currentClusterMeta : currentMetas.values()) {
			try {
				currentClusterMeta.release();
			} catch (Exception e) {
				logger.error("[release] cluster_" + currentClusterMeta.getClusterDbId(), e);
			}
		}
	}

	public void addCluster(final ClusterMeta clusterMeta) {
		CurrentClusterMeta currentClusterMeta = MapUtils.getOrCreate(currentMetas, clusterMeta.getDbId(),
				new ObjectFactory<CurrentClusterMeta>() {

					@Override
					public CurrentClusterMeta create() {
						logger.info("[addCluster][create]cluster_{}:shard_{}", clusterMeta.getId(), clusterMeta.getDbId());
						return new CurrentClusterMeta(clusterMeta.getId(), clusterMeta.getDbId(), clusterMeta.getType());
					}
				});

		for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
			currentClusterMeta.addShard(shardMeta);
		}
	}

	public CurrentClusterMeta removeCluster(Long clusterDbId) {
		CurrentClusterMeta currentClusterMeta = currentMetas.remove(clusterDbId);
		try {
			currentClusterMeta.release();
		} catch (Exception e) {
			logger.error("[remove]cluster_" + clusterDbId, e);
		}
		return currentClusterMeta;
	}

	public void changeCluster(ClusterMetaComparator comparator) {

		Long clusterDbId = comparator.getCurrent().getDbId();
		final CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterDbId);
		if (currentClusterMeta == null) {
			throw new IllegalArgumentException("cluster not exist:" + comparator);
		}

		comparator.accept(new MetaComparatorVisitor<ShardMeta>() {

			@Override
			public void visitRemoved(ShardMeta removed) {
				currentClusterMeta.removeShard(removed);
			}

			@Override
			public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {
				currentClusterMeta.changeShard((ShardMetaComparator) comparator);
			}

			@Override
			public void visitAdded(ShardMeta added) {
				currentClusterMeta.addShard(added);
			}
		});
	}

	public static class CurrentClusterMeta implements Releasable {
		private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();
		@JsonIgnore
		private static Logger logger = LoggerFactory.getLogger(CurrentClusterMeta.class);

		private String clusterId;
		private Long clusterDbId;
		private String clusterType;
		private Map<Long, CurrentShardMeta> clusterMetas = new ConcurrentHashMap<>();
		//map<dc, RouteMeta>
		private Map<String, RouteMeta> outgoingRoutes = new ConcurrentHashMap<>();
//		@JsonIgnore
//		private ChooseRouteStrategy chooseRouteStrategy;
		public CurrentClusterMeta() {

		}

		public CurrentClusterMeta(String clusterId, Long clusterDbId, String clusterType) {
			this.clusterId = clusterId;
			this.clusterDbId = clusterDbId;
			this.clusterType = clusterType;
		}

		public CurrentShardMeta getShard(Long shardDbId) {
			return clusterMetas.get(shardDbId);
		}

		@Override
		public void release() throws Exception {

			logger.info("[release]cluster_{}", clusterDbId);
			for (CurrentShardMeta currentShardMeta : clusterMetas.values()) {
				currentShardMeta.release();
			}

		}

		public void addShard(final ShardMeta shardMeta) {
			MapUtils.getOrCreate(clusterMetas, shardMeta.getDbId(), new ObjectFactory<CurrentShardMeta>() {
				@Override
				public CurrentShardMeta create() {
					logger.info("[addShard][create]cluster_{}, shard_{}, {}", clusterDbId, shardMeta.getDbId(), clusterType);

					switch (ClusterType.lookup(clusterType)) {
						case BI_DIRECTION:
							return new CurrentCRDTShardMeta(clusterDbId, shardMeta.getDbId());
						case ONE_WAY:
							CurrentKeeperShardMeta currentKeeperShardMeta = new CurrentKeeperShardMeta(clusterDbId, shardMeta.getDbId());
							Pair<String, Integer> inetSocketAddress = getDefaultKeeperMaster(shardMeta);
							logger.info("[addShard][default keeper master]{}", inetSocketAddress);
							currentKeeperShardMeta.setKeeperMaster(inetSocketAddress);
							return currentKeeperShardMeta;
						default:
							throw new IllegalArgumentException("unknow type:" + clusterType);
					}
				}
			});
		}

		private Pair<String, Integer> getDefaultKeeperMaster(ShardMeta shardMeta) {

			RedisMeta redisMaster = null;
			for (RedisMeta redisMeta : shardMeta.getRedises()) {
				if (redisMeta.isMaster()) {
					redisMaster = redisMeta;
				}
			}
			if (redisMaster != null) {
				return new Pair<String, Integer>(redisMaster.getIp(), redisMaster.getPort());
			}

			logger.warn("[getDefaultKeeperMaster][no redis master, no upstream, default null]{}", shardMeta);
			return null;
		}

		public void removeShard(ShardMeta shardMeta) {

			CurrentShardMeta currentShardMeta = clusterMetas.remove(shardMeta.getDbId());
			try {
				currentShardMeta.release();
			} catch (Exception e) {
				logger.error("[removeShard]shard_" + shardMeta.getDbId(), e);
			}
		}

		public void changeShard(ShardMetaComparator comparator) {
			CurrentShardMeta currentShardMeta = clusterMetas.get(comparator.getCurrent().getDbId());
			if (currentShardMeta == null) {
				throw new IllegalArgumentException("unfound shard:" + comparator);
			}
			// nothing to do
		}

		public void setClusterId(String clusterId) {
			this.clusterId = clusterId;
		}

		public String getClusterId() {
			return clusterId;
		}

		public Long getClusterDbId() {
			return clusterDbId;
		}

		public String getClusterType() {
			return clusterType;
		}

		//callback changed dc list
		List<String> diffRoutes(Map<String, RouteMeta> current, Map<String, RouteMeta> future) {
			if(current == null || current.size() == 0) return new ArrayList<>(future.keySet());
			if(future == null || future.size() == 0) return new ArrayList<>(current.keySet());

			List<String> changedDcs = new LinkedList<>();
			Set<String> comparedDcs = new HashSet<>();

			for(Map.Entry<String, RouteMeta> entry : current.entrySet()) {
				RouteMeta comparedRouteMeta = future.get(entry.getKey());
				if(comparedRouteMeta == null || !comparedRouteMeta.equals(entry.getValue())) {
					changedDcs.add(entry.getKey());
				}
				comparedDcs.add(entry.getKey());
			}
			for(Map.Entry<String , RouteMeta> entry: future.entrySet()) {
				if(!comparedDcs.contains(entry.getKey())) {
					changedDcs.add(entry.getKey());
				}
			}
			return changedDcs;
		}

		public List<String> updateRoutes(Map<String, RouteMeta> newOutgoingRoutes, ClusterMeta clusterMeta) {
			List<String> changedDcs = diffRoutes(this.outgoingRoutes, newOutgoingRoutes);
			this.outgoingRoutes = newOutgoingRoutes;
			return changedDcs;
		}

		public RouteMeta getRouteByDcId(String dcId) {
			return this.outgoingRoutes.get(dcId.toLowerCase());
		}

		public void setOutgoingRoutes(Map<String, RouteMeta> outgoingRoutes) {
			this.outgoingRoutes = outgoingRoutes;
		}

		public Map<String, RouteMeta> getOutgoingRoutes() {
			return outgoingRoutes;
		}
	}


	@Override
	public String toString() {
		JsonCodec codec = new JsonCodec(true, true);
		return codec.encode(this);
	}

	public static CurrentMeta fromJson(String json) {
		JsonCodec jsonCodec = new JsonCodec(true, true);
		return jsonCodec.decode(json, CurrentMeta.class);
	}

}
