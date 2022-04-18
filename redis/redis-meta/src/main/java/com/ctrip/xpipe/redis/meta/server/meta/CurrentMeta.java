package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.core.util.OrgUtil;
import com.ctrip.xpipe.redis.meta.server.meta.impl.*;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
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

	private static final String CLUSTER_NOT_SUPPORT_APPLIER_TEMPLATE = "cluster: %d, type: %s not support applier";

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

	public boolean watchKeeperIfNotWatched(Long clusterDbId, Long shardDbId) {
		CurrentShardMeta currentShardMeta = currentKeeperShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.watchIfNotWatched();
	}

	public boolean watchApplierIfNotWatched(Long clusterDbId, Long shardDbId) {
		CurrentShardMeta currentShardMeta = currentApplierShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.watchIfNotWatched();
	}

	public void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviveKeepers,
			KeeperMeta activeKeeper) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = currentKeeperShardMetaOrThrowException(clusterDbId, shardDbId);
		currentShardMeta.setSurviveKeepers(surviveKeepers, activeKeeper);

	}

	public void setSurviveAppliers(Long clusterDbId, Long shardDbId, List<ApplierMeta> surviveAppliers,
								  ApplierMeta activeApplier) {
		checkClusterSupportApplier(clusterDbId);

		CurrentApplierShardMeta currentShardMeta = currentApplierShardMetaOrThrowException(clusterDbId, shardDbId);
		currentShardMeta.setSurviveAppliers(surviveAppliers, activeApplier);
	}

	public GtidSet getGtidSet(Long clusterDbId, Long shardDbId, List<RedisMeta> redises, String sids){

		checkClusterSupportApplier(clusterDbId);
		if (StringUtil.isEmpty(sids) || redises == null) {
			return null;
		}

		GtidSet result = null;
		for (RedisMeta redis : redises) {
			GtidSet gtidSet = new GtidSet(redis.getGtid());
			result = result == null? gtidSet: result.intersectionGtidSet(gtidSet);
		}
		if (result == null) {
			logger.warn("[getGtidSet] null, cluster_{},shard_{}", clusterDbId, shardDbId);
			return null;
		}

		Set<String> uuidSet = new HashSet<>();
		String[] sidList = sids.split(",");
		Collections.addAll(uuidSet, sidList);

		return result.filterGtid(uuidSet);
	}

	public void addResource(Long clusterDbId, Long shardDbId, Releasable releasable) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		currentShardMeta.addResource(releasable);

	}

	public List<KeeperMeta> getSurviveKeepers(Long clusterDbId, Long shardDbId) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = currentKeeperShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getSurviveKeepers();
	}

	public List<ApplierMeta> getSurviveAppliers(Long clusterDbId, Long shardDbId) {
		checkClusterSupportApplier(clusterDbId);

		CurrentApplierShardMeta currentShardMeta = currentApplierShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getSurviveAppliers();
	}

	public boolean setKeeperActive(Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = currentKeeperShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.setActiveKeeper(keeperMeta);
	}

	public KeeperMeta getKeeperActive(Long clusterDbId, Long shardDbId) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = currentKeeperShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getActiveKeeper();
	}

	public ApplierMeta getApplierActive(Long clusterDbId, Long shardDbId) {
		checkClusterSupportApplier(clusterDbId);

		CurrentApplierShardMeta currentShardMeta = currentApplierShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getActiveApplier();
	}

	public boolean setKeeperMaster(Long clusterDbId, Long shardDbId, Pair<String, Integer> keeperMaster) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = currentKeeperShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.setKeeperMaster(keeperMaster);
	}

	public boolean setApplierMaster(Long clusterDbId, Long shardDbId, Pair<String, Integer> applierMaster) {
		checkClusterSupportApplier(clusterDbId);

		CurrentApplierShardMeta currentShardMeta = currentApplierShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.setApplierMaster(applierMaster);
	}

	public Pair<String, Integer> getKeeperMaster(Long clusterDbId, Long shardDbId) {
		checkClusterSupportKeeper(clusterDbId);

		CurrentKeeperShardMeta currentShardMeta = currentKeeperShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getKeeperMaster();
	}

	public Pair<String, Integer> getApplierMaster(Long clusterDbId, Long shardDbId) {
		checkClusterSupportApplier(clusterDbId);

		CurrentApplierShardMeta currentShardMeta = currentApplierShardMetaOrThrowException(clusterDbId, shardDbId);
		return currentShardMeta.getApplierMaster();
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
		} else if (currentShardMeta instanceof CurrentHeteroShardMeta) {
			CurrentHeteroShardMeta currentHeteroShardMeta = ((CurrentHeteroShardMeta) currentShardMeta);
			Pair<String, Integer> master = currentHeteroShardMeta.getKeeperShardMeta().getKeeperMaster();
			if (currentHeteroShardMeta.getKeeperShardMeta().getSurviveKeepers().isEmpty()) {
				master = currentHeteroShardMeta.getApplierShardMeta().getApplierMaster();
			}
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

	public List<String> updateClusterRoutes(ClusterMeta clusterMeta, List<RouteMeta> routes) {
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

	private void checkClusterSupportApplier(Long clusterDbId) {
		if (!currentMetas.containsKey(clusterDbId)) return;

		String clusterType = currentMetas.get(clusterDbId).clusterType;
		if (!ClusterType.lookup(clusterType).supportApplier()) {
			throw new IllegalArgumentException(String.format(CLUSTER_NOT_SUPPORT_APPLIER_TEMPLATE, clusterDbId, clusterType));
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

		for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
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
		@JsonIgnore
		private ChooseRouteStrategy chooseRouteStrategy;
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
						case HETERO:
							CurrentKeeperShardMeta keeperShardMeta = new CurrentKeeperShardMeta(clusterDbId, shardMeta.getDbId());
							CurrentApplierShardMeta applierShardMeta = new CurrentApplierShardMeta(clusterDbId, shardMeta.getDbId());
							CurrentHeteroShardMeta currentHeteroShardMeta = new CurrentHeteroShardMeta(clusterDbId, shardMeta.getDbId(),
									keeperShardMeta, applierShardMeta);
							return currentHeteroShardMeta;
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
				this.chooseRouteStrategy = new HashCodeChooseRouteStrategy(clusterDbId.hashCode());
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

	private CurrentKeeperShardMeta currentKeeperShardMetaOrThrowException(Long clusterDbId, Long shardDbId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		if (currentShardMeta instanceof CurrentKeeperShardMeta) {
			return (CurrentKeeperShardMeta) currentShardMeta;
		} else {
			return ((CurrentHeteroShardMeta) currentShardMeta).getKeeperShardMeta();
		}
	}

	private CurrentApplierShardMeta currentApplierShardMetaOrThrowException(Long clusterDbId, Long shardDbId) {
		CurrentShardMeta currentShardMeta = getCurrentShardMetaOrThrowException(clusterDbId, shardDbId);
		return ((CurrentHeteroShardMeta) currentShardMeta).getApplierShardMeta();
	}
}
