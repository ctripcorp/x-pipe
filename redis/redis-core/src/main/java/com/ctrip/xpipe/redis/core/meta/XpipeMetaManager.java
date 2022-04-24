package com.ctrip.xpipe.redis.core.meta;


import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface XpipeMetaManager extends MetaRefUpdateOperation, MetaFieldUpdateOperation, ReadWriteSafe {

	class MetaDesc extends ShardMeta {

		private DcMeta dcMeta;
		private ClusterMeta clusterMeta;
		private ShardMeta shardMeta;
		private Redis redis;

		public MetaDesc(DcMeta dcMeta, ClusterMeta clusterMeta, ShardMeta shardMeta, Redis redis){
			this.dcMeta = dcMeta;
			this.clusterMeta = clusterMeta;
			this.shardMeta = shardMeta;
			this.redis = redis;
		}

		public String getDcId() {
			return dcMeta != null ? dcMeta.getId() : null;
		}

		public String getClusterId() {
			return clusterMeta != null ? clusterMeta.getId() : null;
		}

		public String getShardId() {
			return shardMeta != null ? shardMeta.getId() : null;
		}

		public String getActiveDc(){
			return shardMeta != null ? shardMeta.getActiveDc() : null;
		}

		public Redis getRedis() {
			return redis;
		}
	}
	
	default boolean dcExists(String dc) { return read(()-> doDcExists(dc)); }
	boolean doDcExists(String dc);

	default Set<String> getDcs() { return read(this::doGetDcs); }
	Set<String> doGetDcs();

	default Set<ClusterMeta> getDcClusters(String dc) { return read(() -> doGetDcClusters(dc)); }
	Set<ClusterMeta> doGetDcClusters(String dc);

	default ClusterMeta getClusterMeta(String dc, String clusterId) { return read(()->doGetClusterMeta(dc, clusterId)); }
	ClusterMeta doGetClusterMeta(String dc, String clusterId);

	default ClusterType getClusterType(String clusterId) { return read(()->doGetClusterType(clusterId)); }
	ClusterType doGetClusterType(String clusterId);

	default ShardMeta getShardMeta(String dc, String clusterId, String shardId) { return read(()->doGetShardMeta(dc, clusterId, shardId)); }
	ShardMeta doGetShardMeta(String dc, String clusterId, String shardId);

	default List<KeeperMeta> getKeepers(String dc, String clusterId, String shardId) { return read(()->doGetKeepers(dc, clusterId, shardId)); }
	List<KeeperMeta> doGetKeepers(String dc, String clusterId, String shardId);

	default List<ApplierMeta> getAppliers(String dc, String clusterId, String shardId) { return read(()->doGetAppliers(dc, clusterId, shardId)); }
	List<ApplierMeta> doGetAppliers(String dc, String clusterId, String shardId);

	default List<RedisMeta> getRedises(String dc, String clusterId, String shardId) { return read(()->doGetRedises(dc, clusterId, shardId)); }
	List<RedisMeta> doGetRedises(String dc, String clusterId, String shardId);

	default KeeperMeta getKeeperActive(String dc, String clusterId, String shardId) { return read(()->doGetKeeperActive(dc, clusterId, shardId)); }
	KeeperMeta doGetKeeperActive(String dc, String clusterId, String shardId);

	default List<KeeperMeta> getKeeperBackup(String dc, String clusterId, String shardId) { return read(()->doGetKeeperBackup(dc, clusterId, shardId)); }
	List<KeeperMeta> doGetKeeperBackup(String dc, String clusterId, String shardId);

	default MetaDesc findMetaDesc(HostPort hostPort) { return read(()->doFindMetaDesc(hostPort)); }
	MetaDesc doFindMetaDesc(HostPort hostPort);

	default Pair<String, RedisMeta> getRedisMaster(String clusterId, String shardId) { return read(()->doGetRedisMaster(clusterId, shardId)); }
	Pair<String, RedisMeta> doGetRedisMaster(String clusterId, String shardId);

	default List<MetaServerMeta> getMetaServers(String dc) { return read(()->doGetMetaServers(dc)); }
	List<MetaServerMeta> doGetMetaServers(String dc);

	default SentinelMeta getSentinel(String dc, String clusterId, String shardId) { return read(()->doGetSentinel(dc, clusterId, shardId)); }
	SentinelMeta doGetSentinel(String dc, String clusterId, String shardId);

	default String getSentinelMonitorName(String dc, String clusterId, String shardId) { return read(()->doGetSentinelMonitorName(dc, clusterId, shardId)); }
	String doGetSentinelMonitorName(String dc, String clusterId, String shardId);

	default ZkServerMeta getZkServerMeta(String dc) { return read(()->doGetZkServerMeta(dc)); }
	ZkServerMeta doGetZkServerMeta(String dc);

	default String getActiveDc(String clusterId, String shardId) throws MetaException { return read(()->doGetActiveDc(clusterId, shardId)); }
	String doGetActiveDc(String clusterId, String shardId);

	default Set<String> getBackupDcs(String clusterId, String shardId) { return read(()->doGetBackupDcs(clusterId, shardId)); }
	Set<String> doGetBackupDcs(String clusterId, String shardId);

	default Set<String> getDownstreamDcs(String dc, String clusterId, String shardId) { return read(()->doGetDownstreamDcs(dc, clusterId, shardId)); }
	Set<String> doGetDownstreamDcs(String dc, String clusterId, String shardId);

	default String getUpstreamDc(String dc, String clusterId, String shardId){ return read(()->doGetUpstreamDc(dc, clusterId, shardId)); }
	String doGetUpstreamDc(String dc, String clusterId, String shardId);

	default String getSrcDc(String dc, String clusterId, String shardId){ return read(()->doGetSrcDc(dc, clusterId, shardId)); }
	String doGetSrcDc(String dc, String clusterId, String shardId);

	default Set<String> getRelatedDcs(String clusterId, String shardId) { return read(()->doGetRelatedDcs(clusterId, shardId)); }
	Set<String> doGetRelatedDcs(String clusterId, String shardId);

	default KeeperContainerMeta getKeeperContainer(String dc, KeeperMeta keeperMeta) { return read(()->doGetKeeperContainer(dc, keeperMeta)); }
	KeeperContainerMeta doGetKeeperContainer(String dc, KeeperMeta keeperMeta);

	default ApplierContainerMeta getApplierContainer(String dc, ApplierMeta applierMeta) { return read(()->doGetApplierContainer(dc, applierMeta)); }
	ApplierContainerMeta doGetApplierContainer(String dc, ApplierMeta applierMeta);

	default DcMeta getDcMeta(String dc) { return read(()->doGetDcMeta(dc)); }
	DcMeta doGetDcMeta(String dc);

	default String getDcZone(String dc) { return read(()->doGetDcZone(dc)); }
	String doGetDcZone(String dc);

	default List<KeeperMeta> getAllSurviveKeepers(String currentDc, String clusterId, String shardId) { return read(()-> doGetAllSurviveKeepers(currentDc, clusterId, shardId)); }
	List<KeeperMeta> doGetAllSurviveKeepers(String currentDc, String clusterId, String shardId);

	default boolean hasCluster(String currentDc, String clusterId) { return read(()->doHasCluster(currentDc, clusterId)); }
	boolean doHasCluster(String currentDc, String clusterId);

	default boolean hasShard(String currentDc, String clusterId, String shardId) { return read(()->doHasShard(currentDc, clusterId, shardId)); }
	boolean doHasShard(String currentDc, String clusterId, String shardId);

	default List<RouteMeta> routes(String currentDc, String tag) { return read(()->doGetRoutes(currentDc, tag)); }
	List<RouteMeta> doGetRoutes(String currentDc, String tag);

	default RouteMeta randomRoute(String currentDc, String tag, Integer orgId, String dstDc) { return read(()->doGetRandomRoute(currentDc, tag, orgId, dstDc)); }
	RouteMeta doGetRandomRoute(String currentDc, String tag, Integer orgId, String dstDc);

	default List<RouteMeta>  metaRoutes(String currentDc){
		return routes(currentDc, Route.TAG_META);
	}

	default RouteMeta metaRandomRoutes(String currentDc, Integer orgId, String dstDc){
		return randomRoute(currentDc, Route.TAG_META, orgId, dstDc);
	}

	default List<RouteMeta> consoleRoutes(String currentDc) {
		return routes(currentDc, Route.TAG_CONSOLE);
	}

	Integer ORG_ID_FOR_SHARED_ROUTES = 0;

	default List<ClusterMeta> getSpecificActiveDcClusters(String currentDc, String clusterActiveDc) { return read(()->doGetSpecificActiveDcClusters(currentDc, clusterActiveDc)); }
	List<ClusterMeta> doGetSpecificActiveDcClusters(String currentDc, String clusterActiveDc);
}
