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
import com.ctrip.xpipe.redis.core.util.OrgUtil;
import com.ctrip.xpipe.redis.meta.server.meta.impl.CurrentCRDTShardMeta;
import com.ctrip.xpipe.redis.meta.server.meta.impl.CurrentKeeperShardMeta;
import com.ctrip.xpipe.redis.meta.server.meta.impl.HashCodeChooseRouteStrategy;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *
 *         Sep 6, 2016
 */
public class CurrentMeta implements Releasable {

	@JsonIgnore
	private Logger logger = LoggerFactory.getLogger(CurrentMeta.class);

	private Map<String, CurrentClusterMeta> currentMetas = new ConcurrentHashMap<>();

	private static final String CLUSTER_NOT_SUPPORT_KEEPER_TEMPLATE = "cluster: %s, type: %s not support keeper";

	private static final String CLUSTER_NOT_SUPPORT_PEER_MASTER_TEMPLATE = "cluster: %s, type: %s not support peer master";

	public Set<String> allClusters() {
		return new HashSet<>(currentMetas.keySet());
	}

	public boolean hasCluster(String clusterId) {
		return currentMetas.get(clusterId) != null;
	}

	public boolean hasShard(String clusterId, String shardId) {
		return getCurrentShardMeta(clusterId, shardId) != null;
	}

	public boolean watchIfNotWatched(String clusterId, String shardId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.watchIfNotWatched();
	}

	public void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviveKeepers,
			KeeperMeta activeKeeper) {
		checkClusterSupportKeeper(clusterId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentShardMeta.setSurviveKeepers(surviveKeepers, activeKeeper);

	}

	public void addResource(String clusterId, String shardId, Releasable releasable) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentShardMeta.addResource(releasable);

	}

	public List<KeeperMeta> getSurviveKeepers(String clusterId, String shardId) {
		checkClusterSupportKeeper(clusterId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.getSurviveKeepers();
	}

	public boolean setKeeperActive(String clusterId, String shardId, KeeperMeta keeperMeta) {
		checkClusterSupportKeeper(clusterId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.setActiveKeeper(keeperMeta);
	}

	public KeeperMeta getKeeperActive(String clusterId, String shardId) {
		checkClusterSupportKeeper(clusterId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.getActiveKeeper();
	}

	public boolean setKeeperMaster(String clusterId, String shardId, Pair<String, Integer> keeperMaster) {
		checkClusterSupportKeeper(clusterId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.setKeeperMaster(keeperMaster);
	}

	public Pair<String, Integer> getKeeperMaster(String clusterId, String shardId) {
		checkClusterSupportKeeper(clusterId);

		CurrentKeeperShardMeta currentShardMeta = (CurrentKeeperShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentShardMeta.getKeeperMaster();
	}

	public void setCurrentCRDTMaster(String clusterId, String shardId,  RedisMeta peerMaster) {
		checkClusterSupportPeerMaster(clusterId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentCRDTShardMeta.setCurrentMaster(peerMaster);
	}

	public RedisMeta getCurrentCRDTMaster(String clusterId, String shardId) {
		checkClusterSupportPeerMaster(clusterId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentCRDTShardMeta.getCurrentMaster();
	}

	public RedisMeta getCurrentMaster(String clusterId, String shardId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterId, shardId);
		if (currentShardMeta instanceof CurrentCRDTShardMeta) {
			return ((CurrentCRDTShardMeta) currentShardMeta).getCurrentMaster();
		} else if (currentShardMeta instanceof CurrentKeeperShardMeta) {
			Pair<String, Integer> master = ((CurrentKeeperShardMeta) currentShardMeta).getKeeperMaster();
			if (null == master) return null;
			return new RedisMeta().setIp(master.getKey()).setPort(master.getValue());
		}

		return null;
	}

	public void setPeerMaster(String dcId, String clusterId, String shardId, RedisMeta peerMaster) {
		checkClusterSupportPeerMaster(clusterId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentCRDTShardMeta.setPeerMaster(dcId, peerMaster);
	}

	public RedisMeta getPeerMaster(String dcId, String clusterId, String shardId) {
		checkClusterSupportPeerMaster(clusterId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentCRDTShardMeta.getPeerMaster(dcId);
	}

	public RouteMeta getClusterRouteByDcId(String clusterId, String dcId) {
		CurrentClusterMeta clusterMeta = currentMetas.get(clusterId);
		return clusterMeta.getRouteByDcId(dcId);
	}

	public List<String> updateClusterRoutes(ClusterMeta clusterMeta, List<RouteMeta> routes) {
		CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterMeta.getId());
		return currentClusterMeta.updateRoutes(routes, clusterMeta);
	}

	public void removePeerMaster(String dcId, String clusterId, String shardId) {
		checkClusterSupportPeerMaster(clusterId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		currentCRDTShardMeta.removePeerMaster(dcId);
	}

	public Set<String> getUpstreamPeerDcs(String clusterId, String shardId) {
		checkClusterSupportPeerMaster(clusterId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentCRDTShardMeta.getUpstreamPeerDcs();
	}

	public Map<String, RedisMeta> getAllPeerMasters(String clusterId, String shardId) {
		checkClusterSupportPeerMaster(clusterId);

		CurrentCRDTShardMeta currentCRDTShardMeta = (CurrentCRDTShardMeta) getCurrentShardMetaOrThrowException(clusterId, shardId);
		return currentCRDTShardMeta.getAllPeerMasters();
	}

	private void checkClusterSupportKeeper(String clusterId) {
		if (!currentMetas.containsKey(clusterId)) return;

		String clusterType = currentMetas.get(clusterId).clusterType;
		if (!ClusterType.lookup(clusterType).supportKeeper()) {
			throw new IllegalArgumentException(String.format(CLUSTER_NOT_SUPPORT_KEEPER_TEMPLATE, clusterId, clusterType));
		}
	}

	private void checkClusterSupportPeerMaster(String clusterId) {
		if (!currentMetas.containsKey(clusterId)) return;

		String clusterType = currentMetas.get(clusterId).clusterType;
		if (!ClusterType.lookup(clusterType).supportMultiActiveDC()) {
			throw new IllegalArgumentException(String.format(CLUSTER_NOT_SUPPORT_PEER_MASTER_TEMPLATE, clusterId, clusterType));
		}
	}

	private CurrentShardMeta getCurrentShardMetaOrThrowException(String clusterId, String shardId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMeta(clusterId, shardId);
		if (currentShardMeta == null) {
			throw new IllegalArgumentException("can not find :" + clusterId + "," + shardId);
		}
		return currentShardMeta;
	}

	private CurrentShardMeta getCurrentShardMeta(String clusterId, String shardId) {
		CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterId);
		if (currentClusterMeta == null) {
			return null;
		}
		CurrentShardMeta currentShardMeta = currentClusterMeta.getShard(shardId);
		return currentShardMeta;
	}

	@Override
	public void release() throws Exception {

		for (CurrentClusterMeta currentClusterMeta : currentMetas.values()) {
			try {
				currentClusterMeta.release();
			} catch (Exception e) {
				logger.error("[release]" + currentClusterMeta.getClusterId(), e);
			}
		}
	}

	public void addCluster(final ClusterMeta clusterMeta) {
		CurrentClusterMeta currentClusterMeta = MapUtils.getOrCreate(currentMetas, clusterMeta.getId(),
				new ObjectFactory<CurrentClusterMeta>() {

					@Override
					public CurrentClusterMeta create() {
						logger.info("[addCluster][create]{}", clusterMeta.getId());
						return new CurrentClusterMeta(clusterMeta.getId(), clusterMeta.getType());
					}
				});

		for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
			currentClusterMeta.addShard(shardMeta);
		}
	}

	public CurrentClusterMeta removeCluster(String clusterId) {
		CurrentClusterMeta currentClusterMeta = currentMetas.remove(clusterId);
		try {
			currentClusterMeta.release();
		} catch (Exception e) {
			logger.error("[remove]" + clusterId, e);
		}
		return currentClusterMeta;
	}

	public void changeCluster(ClusterMetaComparator comparator) {

		String clusterId = comparator.getCurrent().getId();
		final CurrentClusterMeta currentClusterMeta = currentMetas.get(clusterId);
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
		private String clusterType;
		private Map<String, CurrentShardMeta> clusterMetas = new ConcurrentHashMap<>();
		//map<dc, RouteMeta>
		private Map<String, RouteMeta> outgoingRoutes = new ConcurrentHashMap<>();
		@JsonIgnore
		private ChooseRouteStrategy chooseRouteStrategy;
		public CurrentClusterMeta() {

		}

		public CurrentClusterMeta(String clusterId, String clusterType) {
			this.clusterId = clusterId;
			this.clusterType = clusterType;
		}

		public CurrentShardMeta getShard(String shardId) {
			return clusterMetas.get(shardId);
		}

		@Override
		public void release() throws Exception {

			logger.info("[release]{}", clusterId);
			for (CurrentShardMeta currentShardMeta : clusterMetas.values()) {
				currentShardMeta.release();
			}

		}

		public void addShard(final ShardMeta shardMeta) {
			MapUtils.getOrCreate(clusterMetas, shardMeta.getId(), new ObjectFactory<CurrentShardMeta>() {
				@Override
				public CurrentShardMeta create() {
					logger.info("[addShard][create]{} , {}, {}", clusterId, shardMeta.getId(), clusterType);

					switch (ClusterType.lookup(clusterType)) {
						case BI_DIRECTION:
							return new CurrentCRDTShardMeta(clusterId, shardMeta.getId());
						case ONE_WAY:
							CurrentKeeperShardMeta currentKeeperShardMeta = new CurrentKeeperShardMeta(clusterId, shardMeta.getId());
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

			CurrentShardMeta currentShardMeta = clusterMetas.remove(shardMeta.getId());
			try {
				currentShardMeta.release();
			} catch (Exception e) {
				logger.error("[removeShard]" + shardMeta.getId(), e);
			}
		}

		public void changeShard(ShardMetaComparator comparator) {
			CurrentShardMeta currentShardMeta = clusterMetas.get(comparator.getCurrent().getId());
			if (currentShardMeta == null) {
				throw new IllegalArgumentException("unfound shard:" + comparator);
			}
			// nothing to do
		}

		public String getClusterId() {
			return clusterId;
		}

		public String getClusterType() {
			return clusterType;
		}

		private RouteMeta chooseRoute(Integer orgId, List<RouteMeta> dstDcRoutes, ChooseRouteStrategy strategy) {
			if(dstDcRoutes == null) return null;
			List<RouteMeta> resultsCandidates = new LinkedList<>();
			dstDcRoutes.forEach(routeMeta -> {
				if(ObjectUtils.equals(routeMeta.getOrgId(), orgId)){
					resultsCandidates.add(routeMeta);
				}
			});

			if(!resultsCandidates.isEmpty()){
				return strategy.choose(resultsCandidates);
			}


			dstDcRoutes.forEach(routeMeta -> {
				if(OrgUtil.isDefaultOrg(routeMeta.getOrgId())){
					resultsCandidates.add(routeMeta);
				}
			});

			return strategy.choose(resultsCandidates);
		}

		private Map<String, RouteMeta> chooseRoutes(List<RouteMeta> routes, ClusterMeta clusterMeta) {
			Map<String, RouteMeta> allRoutes = new ConcurrentHashMap<>();
			if(routes == null || routes.isEmpty()){
				return allRoutes;
			}
			logger.debug("routes: {}", routes);
			Map<String, List<RouteMeta>> allDcRoutes = new ConcurrentHashMap<>();
			routes.forEach(routeMeta -> {
				String dcName = routeMeta.getDstDc();
				List<RouteMeta> dcRoutes = MapUtils.getOrCreate(allDcRoutes, dcName, LinkedList::new);
				dcRoutes.add(routeMeta);
			});
			Integer orgId = clusterMeta.getOrgId();
			if(ClusterType.lookup(clusterType).supportMultiActiveDC()) {
				String dcs = clusterMeta.getDcs();
				if(StringUtil.isEmpty(dcs)) return allRoutes;
				for (String dcId : dcs.split("\\s*,\\s*")) {
					if (currentDcId.equalsIgnoreCase(dcId)) continue;
					RouteMeta route = chooseRoute(orgId, allDcRoutes.get(dcId), this.getChooseRouteStrategy());
					if(route != null) allRoutes.put(dcId.toLowerCase(), route);
				}
			} else {
				String dcId = clusterMeta.getActiveDc();
				if(!currentDcId.equalsIgnoreCase(dcId)) {
					RouteMeta route = chooseRoute(orgId, allDcRoutes.get(dcId), this.getChooseRouteStrategy());
					if(route != null) allRoutes.put(dcId.toLowerCase(), route);
				}
			}
			return allRoutes;
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

		public List<String> updateRoutes(List<RouteMeta> routes, ClusterMeta clusterMeta) {
			Map<String, RouteMeta> outgoingRoutes = chooseRoutes(routes, clusterMeta);
			List<String> changedDcs = diffRoutes(this.outgoingRoutes, outgoingRoutes);
			this.outgoingRoutes = outgoingRoutes;
			return changedDcs;
		}

		public RouteMeta getRouteByDcId(String dcId) {
			return this.outgoingRoutes.get(dcId.toLowerCase());
		}

		public void setOutgoingRoutes(Map<String, RouteMeta> outgoingRoutes) {
			this.outgoingRoutes = outgoingRoutes;
		}

		public void setChooseRouteStrategy(ChooseRouteStrategy chooseRouteStrategy) {
			this.chooseRouteStrategy = chooseRouteStrategy;
		}

		public ChooseRouteStrategy getChooseRouteStrategy() {
			if(chooseRouteStrategy == null) {
				this.chooseRouteStrategy = new HashCodeChooseRouteStrategy(clusterId.hashCode());
			}
			return this.chooseRouteStrategy;
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
