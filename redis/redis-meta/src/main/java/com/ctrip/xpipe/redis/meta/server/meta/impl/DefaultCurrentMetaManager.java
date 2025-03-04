package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMeta;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentShardMeta;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
@Component
public class DefaultCurrentMetaManager extends AbstractLifecycleObservable implements CurrentMetaManager, Observer{
	
	private int slotCheckInterval = 60;
	
	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	private CurrentClusterServer currentClusterServer;
	
	@Autowired
	private DcMetaCache dcMetaCache;
	
	private CurrentMeta currentMeta = new CurrentMeta();
	
	private Set<Integer>   currentSlots = new HashSet<>();

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	private ScheduledFuture<?> 		slotCheckFuture;
	
	@Autowired
	private List<MetaServerStateChangeHandler> stateHandlers;

	@Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
	private Executor executors;

	public DefaultCurrentMetaManager() {
	}
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

		setExecutors(executors);

		logger.info("[doInitialize]{}, {}", stateHandlers, currentClusterServer.getServerId());
		dcMetaCache.addObserver(this);
	}
	
	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		for(Integer slotId : currentClusterServer.slots()){
			addSlot(slotId);
		}
		
		
		slotCheckFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() {
				checkAddOrRemoveSlots();
				checkCurrentMetaMissOrRedundant();
			}
		}, slotCheckInterval, slotCheckInterval, TimeUnit.SECONDS);
	}
	
	@Override
	public synchronized void addObserver(Observer observer) {
		logger.info("[addObserver]{}", observer);
		super.addObserver(observer);
	}

	
	protected void checkAddOrRemoveSlots() {
		
		Set<Integer> slots = slotManager.getSlotsByServerId(currentClusterServer.getServerId(), false);
		
		Pair<Set<Integer>, Set<Integer>> result = getAddAndRemove(slots, currentSlots);
		
		for(Integer slotId : result.getKey()){
			addSlot(slotId);
		}

		for(Integer slotId : result.getValue()){
			deleteSlot(slotId);
		}
	}

	protected void checkCurrentMetaMissOrRedundant() {
		logger.debug("[checkCurrentMetaMissOrRedundant] begin");
		Set<ClusterMeta> clusters = dcMetaCache.getClusters();
		Set<Long> clusterDbIdsInMeta = clusters.stream().map(ClusterMeta::getDbId).collect(Collectors.toSet());
		for (ClusterMeta clusterMeta: clusters) {
			if (currentClusterServer.hasKey(clusterMeta.getDbId()) && !currentMeta.hasCluster(clusterMeta.getDbId())) {
				logger.warn("[checkCurrentMeta][miss cluster]{}:{}", clusterMeta.getId(), clusterMeta.getDbId());
				addCluster(clusterMeta.getDbId());
			} else if (!currentClusterServer.hasKey(clusterMeta.getDbId()) && currentMeta.hasCluster(clusterMeta.getDbId())) {
				logger.warn("[checkCurrentMeta][redundant cluster]{}:{}", clusterMeta.getId(), clusterMeta.getDbId());
				removeClusterInterested(clusterMeta.getDbId());
			}
		}

		for (Long clusterDbId: currentMeta.allClusters()) {
			if (!clusterDbIdsInMeta.contains(clusterDbId)) {
				logger.warn("[checkCurrentMetaMissOrRedundant][unexpected current cluster] cluster_{}", clusterDbId);
				EventMonitor.DEFAULT.logAlertEvent("[unexpected current cluster] cluster_" + clusterDbId);
			}
		}
	}

	protected Pair<Set<Integer>, Set<Integer>> getAddAndRemove(Set<Integer> future, Set<Integer> current) {
		
		Set<Integer> added = new HashSet<>(future);
		added.removeAll(current);

		if(added.size() > 0){
			logger.info("[checkAddOrRemoveSlots][to add]{}", added);
		}
		
		Set<Integer> toRemove = new HashSet<>(current);
		toRemove.removeAll(future);

		if(toRemove.size() > 0){
			logger.info("[checkAddOrRemoveSlots][toRemove]{}", toRemove);
		}
		
		return new Pair<>(added, toRemove);
	}


	@Override
	protected void doStop() throws Exception {
		
		slotCheckFuture.cancel(true);
		super.doStop();
	}
	
	
	@Override
	protected void doDispose() throws Exception {

		currentMeta.release();
		super.doDispose();
	}

	private void clusterRoutesChange(Long clusterDbId) {
		ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterDbId);
		if (ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.BI_DIRECTION)) {
			refreshPeerMasters(clusterMeta);
		} else if (ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.ONE_WAY)) {
			refreshKeeperMaster(clusterMeta);
			refreshApplierMaster(clusterMeta);
		}
	}
	
	private boolean needUpdateClusterRoutesWhenClusterChange(ClusterMeta current, ClusterMeta future) {
		ClusterType clusterType = ClusterType.lookup(future.getType());
		if(clusterType == null) {
			logger.error("[unknown cluster type] cluster id: {}, type :{}", future.getId(), future.getType());
			return false;
		}

		if(isClusterDesignatedRouteChanged(current.getClusterDesignatedRouteIds(), future.getClusterDesignatedRouteIds()))
			return true;

		if(clusterType.supportMultiActiveDC()) {
			//arraylist join ","  
			return !ObjectUtils.equals(current.getDcs(), future.getDcs());
		} else {
			return !ObjectUtils.equals(current.getActiveDc(), future.getActiveDc());
		}
	}

	private boolean isClusterDesignatedRouteChanged(String currentDesignatedRoutes, String futureDesignatedRoutes) {
		if(currentDesignatedRoutes == null && futureDesignatedRoutes == null) return false;
		if(currentDesignatedRoutes == null || futureDesignatedRoutes == null) return true;

		return !Objects.equals(Sets.newHashSet(currentDesignatedRoutes.split(",")), Sets.newHashSet(futureDesignatedRoutes.split(",")));
	}

	private void handleClusterChanged(ClusterMetaComparator clusterMetaComparator) {
		Long clusterDbId = clusterMetaComparator.getCurrent().getDbId();
		if(currentMeta.hasCluster(clusterDbId)){
			ClusterMeta current = clusterMetaComparator.getCurrent();
			ClusterMeta future = clusterMetaComparator.getFuture();
			currentMeta.changeCluster(clusterMetaComparator);
			if(needUpdateClusterRoutesWhenClusterChange(current, future)) {
				clusterRoutesChange(clusterDbId);
			}
			notifyObservers(clusterMetaComparator);

		}else{
			logger.warn("[handleClusterChanged][but we do not has it]{}", clusterMetaComparator);
			addCluster(clusterDbId);
		}
	}


	@VisibleForTesting
	protected synchronized void addCluster(Long clusterDbId) {
		if (currentMeta.hasCluster(clusterDbId)) {
			logger.info("[addCluster][already exist]cluster_{}", clusterDbId);
			return;
		}
		ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterDbId);
		if (null == clusterMeta) {
			logger.info("[addCluster][unfound]cluster_{}", clusterDbId);
			return;
		}

		logger.info("[addCluster]{}:{}, {}", clusterMeta.getId(), clusterDbId, clusterMeta);
		currentMeta.addCluster(clusterMeta);
		notifyObservers(new NodeAdded<ClusterMeta>(clusterMeta));
	}

	private void destroyCluster(ClusterMeta clusterMeta){
		//keeper in clustermeta, keepermanager remove keeper
		removeCluster(clusterMeta);
	}
	
	private synchronized void removeCluster(ClusterMeta clusterMeta) {
		
		logger.info("[removeCluster] {}", clusterMeta.getDbId());
		boolean result = currentMeta.removeCluster(clusterMeta.getDbId()) != null;
		if(result){
			notifyObservers(new NodeDeleted<ClusterMeta>(clusterMeta));
		}
	}



	private void removeClusterInterested(Long clusterDbId) {
		ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterDbId);
		if (null == clusterMeta) {
			logger.info("[removeClusterInterested][unfound, remove anyway]cluster_{}", clusterDbId);
			removeCluster(new ClusterMeta().setDbId(clusterDbId));
		} else {
			removeCluster(new ClusterMeta().setType(clusterMeta.getType()).setDbId(clusterDbId));
		}
	}

	@Override
	public Set<Long> allClusters() {
		return new HashSet<>(currentMeta.allClusters());
	}

	@Override
	public void deleteSlot(int slotId) {
		
		currentSlots.remove(slotId);
		logger.info("[deleteSlot]{}", slotId);
		for(Long clusterDbId: currentMeta.allClusters()){
			int currentSlotId = slotManager.getSlotIdByKey(clusterDbId);
			if(currentSlotId == slotId){
				removeClusterInterested(clusterDbId);
			}
		}
	}


	@Override
	public void addSlot(int slotId) {
		
		logger.info("[addSlot]{}", slotId);
		currentSlots.add(slotId);
		for(ClusterMeta clusterMeta : dcMetaCache.getClusters()){
			Long clusterDbId = clusterMeta.getDbId();
			int currentSlotId = slotManager.getSlotIdByKey(clusterDbId);
			if(currentSlotId == slotId){
				addCluster(clusterDbId);
			}
		}
	}

	@Override
	public void exportSlot(int slotId) {
		
		logger.info("[exportSlot]{}", slotId);
		deleteSlot(slotId);
	}

	@Override
	public void importSlot(int slotId) {
		
		logger.info("[importSlot][doNothing]{}", slotId);
	}

	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof DcMetaComparator){
			
			dcMetaChange((DcMetaComparator)args);
		} else if(args instanceof DcRouteMetaComparator) {
			logger.debug("[routeChanges]{}", (DcRouteMetaComparator) args);
			routeChanges();
		} else{
			
			throw new IllegalArgumentException(String.format("unknown args(%s):%s", args.getClass(), args));
		}
	}
	
	@VisibleForTesting
	protected void setCurrentClusterServer(CurrentClusterServer currentClusterServer) {
		this.currentClusterServer = currentClusterServer;
	}
	
	@VisibleForTesting
	protected void dcMetaChange(DcMetaComparator comparator) {
		
		for(ClusterMeta clusterMeta : comparator.getAdded()){
			if(currentClusterServer.hasKey(clusterMeta.getDbId())){
				addCluster(clusterMeta.getDbId());
			}else{
				logger.info("[dcMetaChange][add][not interested]{}", clusterMeta.getId());
			}
		}
		
		for(ClusterMeta clusterMeta : comparator.getRemoved()){
			if(currentClusterServer.hasKey(clusterMeta.getDbId())){
				destroyCluster(clusterMeta);
			}else{
				logger.info("[dcMetaChange][destroy][not interested]{}", clusterMeta.getId());
			}

		}
		
		for(@SuppressWarnings("rawtypes") MetaComparator changedComparator : comparator.getMofified()){
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) changedComparator;
			Long clusterDbId = clusterMetaComparator.getCurrent().getDbId();
			if(currentClusterServer.hasKey(clusterDbId)){
				handleClusterChanged(clusterMetaComparator);
			}else{
				logger.info("[dcMetaChange][change][not interested]{}", clusterDbId);
			}
		}
	}

	@VisibleForTesting
	protected void routeChanges() {
		for(Long clusterDbId : allClusters()) {
			clusterRoutesChange(clusterDbId);
		}
	}

	protected void refreshPeerMasters(ClusterMeta clusterMeta) {
		for (ShardMeta shard : clusterMeta.getShards().values()) {
			notifyPeerMasterChange(clusterMeta.getDbId(), shard.getDbId());
		}
	}

	@VisibleForTesting
	protected void refreshKeeperMaster(ClusterMeta clusterMeta) {
		Collection<ShardMeta> shards = clusterMeta.getAllShards().values();
		Long clusterDbId = clusterMeta.getDbId();
		for (ShardMeta shard : shards) {
			notifyKeeperMasterChanged(clusterDbId, shard.getDbId(), getKeeperMaster(clusterDbId, shard.getDbId()));
		}
	}

	protected void refreshApplierMaster(ClusterMeta clusterMeta) {
		Collection<ShardMeta> shards = clusterMeta.getAllShards().values();
		Long clusterDbId = clusterMeta.getDbId();
		for (ShardMeta shard : shards) {
		    String sids = this.getSrcSids(clusterDbId, shard.getDbId());
			notifyApplierMasterChanged(clusterDbId, shard.getDbId(), getApplierMaster(clusterDbId, shard.getDbId()), sids);
		}
	}
	
	@Override
	public boolean hasCluster(Long clusterDbId) {
		return currentMeta.hasCluster(clusterDbId);
	}

	@Override
	public boolean hasShard(Long clusterDbId, Long shardDbId) {
		return currentMeta.hasShard(clusterDbId, shardDbId);
	}
	
	@Override
	public RedisMeta getRedisMaster(Long clusterDbId, Long shardDbId) {
		return ((DefaultDcMetaCache)dcMetaCache).getDcMeta().getRedisMaster(clusterDbId, shardDbId);
	}

	@Override
	public ClusterMeta getClusterMeta(Long clusterDbId) {
		return dcMetaCache.getClusterMeta(clusterDbId);
	}

	@Override
	public Pair<String, Integer> getKeeperMaster(Long clusterDbId, Long shardDbId) {
		return currentMeta.getKeeperMaster(clusterDbId, shardDbId);
	}

	@Override
	public Pair<String, Integer> getApplierMaster(Long clusterDbId, Long shardDbId) {
		return currentMeta.getApplierMaster(clusterDbId, shardDbId);
	}

	@Override
	public List<KeeperMeta> getSurviveKeepers(Long clusterDbId, Long shardDbId) {
		return currentMeta.getSurviveKeepers(clusterDbId, shardDbId);
	}

	@Override
	public List<ApplierMeta> getSurviveAppliers(Long clusterDbId, Long shardDbId) {
	    return currentMeta.getSurviveAppliers(clusterDbId, shardDbId);
	}

	@Override
	public List<RedisMeta> getRedises(Long clusterDbId, Long shardDbId) {
	    return currentMeta.getRedises(clusterDbId, shardDbId);
	}

	@Override
	public KeeperMeta getKeeperActive(Long clusterDbId, Long shardDbId) {
		return currentMeta.getKeeperActive(clusterDbId, shardDbId);
	}

	@Override
	public ApplierMeta getApplierActive(Long clusterDbId, Long shardDbId) {
		return currentMeta.getApplierActive(clusterDbId, shardDbId);
	}

	@Override
	public String getCurrentMetaDesc() {
	
		Map<String, Object> desc = new HashMap<>();
		desc.put("meta", currentMeta);
		desc.put("currentSlots", currentSlots);
		JsonCodec codec = new JsonCodec(true, true);
		return codec.encode(desc);
	}

	@Override
	public List<KeeperMeta> getOneWaySurviveKeepers(Long clusterDbId, Long shardDbId) {
		return currentMeta.getOneWaySurviveKeepers(clusterDbId, shardDbId);
	}

	protected Set<Integer> getCurrentSlots() {
		return currentSlots;
	}


	/*******************update dynamic info*************************/

	@Override
	public void setRedises(Long clusterDbId, Long shardDbId, List<RedisMeta> redises) {
		currentMeta.setRedises(clusterDbId, shardDbId, redises);
	}

	@Override
	public boolean updateKeeperActive(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper) {
		boolean result = currentMeta.setKeeperActive(clusterDbId, shardDbId, activeKeeper);
		notifyKeeperActiveElected(clusterDbId, shardDbId, activeKeeper);
		return result;
	}
	
	@Override
	public void addResource(Long clusterDbId, Long shardDbId, Releasable releasable) {
		currentMeta.addResource(clusterDbId, shardDbId, releasable);
	}


	@Override
	public void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviveKeepers, KeeperMeta activeKeeper) {
		if (!currentMeta.setSurviveKeepers(clusterDbId, shardDbId, surviveKeepers, activeKeeper)) return ;
		notifyKeeperActiveElected(clusterDbId, shardDbId, activeKeeper);
	}

	@Override
	public void setSurviveAppliersAndNotify(Long clusterDbId, Long shardDbId, List<ApplierMeta> surviveAppliers, ApplierMeta activeApplier, String sids) {
		currentMeta.setSurviveAppliers(clusterDbId, shardDbId, surviveAppliers, activeApplier);
		notifyApplierActiveElected(clusterDbId, shardDbId, activeApplier, sids);
	}

	@Override
	public GtidSet getGtidSet(Long clusterDbId, String srcSids) {
		return currentMeta.getGtidSet(clusterDbId, srcSids);
	}

	@Override
	public String getSids(Long clusterDbId, Long shardDbId) {
	    return currentMeta.getSids(clusterDbId, shardDbId);
	}

	@Override
	public String getSrcSids(Long clusterDbId, Long shardDbId) {
	    return currentMeta.getSrcSids(clusterDbId, shardDbId);
	}

	@Override
	public void setKeeperMaster(Long clusterDbId, Long shardDbId, String ip, int port) {
		setKeeperMaster(clusterDbId, shardDbId, ip, port, null);
	}

	@Override
	public void setKeeperMaster(Long clusterDbId, Long shardDbId, String ip, int port, String expectedPrimaryDc) {
		String dcName = dcMetaCache.getPrimaryDc(clusterDbId, shardDbId);
		if(expectedPrimaryDc != null && !StringUtil.trimEquals(dcName, expectedPrimaryDc)) {
			// 如果 expectedDc 为null, 不进行校验。发生了dr切换，禁止修改。
			// 如果任务基于 PrimaryDc 来修改 keeper meta 就要校验检测过程是否 dc 切换。
			logger.info("[setKeeperMaster][rejected] primaryDc:{}, expectedPrimaryDc:{}", dcName, expectedPrimaryDc);
			return;
		}
		Pair<String, Integer> keeperMaster = new Pair<String, Integer>(ip, port);
		if(currentMeta.setKeeperMaster(clusterDbId, shardDbId, keeperMaster)){
			logger.info("[setKeeperMaster]cluster_{},shard_{},{}:{}", clusterDbId, shardDbId, ip, port);
			notifyKeeperMasterChanged(clusterDbId, shardDbId, keeperMaster);
		}else{
			logger.info("[setKeeperMaster][keeper master not changed!]cluster_{},shard_{},{}:{}", clusterDbId, shardDbId, ip, port);
		}
		
	}

	@Override
	public void setApplierMasterAndNotify(Long clusterDbId, Long shardDbId, String ip, int port, String sids) {

		Pair<String, Integer> applierMaster = new Pair<String, Integer>(ip, port);
		if(currentMeta.setApplierMaster(clusterDbId, shardDbId, applierMaster)){
			logger.info("[setApplierMaster]cluster_{},shard_{},{}:{}", clusterDbId, shardDbId, ip, port);
			notifyApplierMasterChanged(clusterDbId, shardDbId, applierMaster, sids);
		}else{
			logger.info("[setApplierMaster][applier master not changed!]cluster_{},shard_{},{}:{}", clusterDbId, shardDbId, ip, port);
		}

	}

	@Override
	public void setSrcSidsAndNotify(Long clusterDbId, Long shardDbId, String sids) {
		if (currentMeta.setSrcSids(clusterDbId, shardDbId, sids)) {
			logger.info("[setSrcSids]cluster_{},shard_{},{}", clusterDbId, shardDbId, sids);
			notifyApplierMasterChanged(clusterDbId, shardDbId, currentMeta.getApplierMaster(clusterDbId, shardDbId), sids);
		} else {
			logger.info("[setSrcSids][srcSids not changed!]cluster_{},shard_{},{}", clusterDbId, shardDbId, sids);
		}
	}

	@Override
	public void setKeeperMaster(Long clusterDbId, Long shardDbId, String addr) {
		
		logger.info("[setKeeperMaster]cluster_{},shard_{},{}", clusterDbId, shardDbId, addr);
		Pair<String, Integer> inetAddr = IpUtils.parseSingleAsPair(addr);
		setKeeperMaster(clusterDbId, shardDbId, inetAddr.getKey(), inetAddr.getValue());
	}

	@Override
	public boolean watchKeeperIfNotWatched(Long clusterDbId, Long shardDbId) {
		return currentMeta.watchKeeperIfNotWatched(clusterDbId, shardDbId);
	}

	@Override
	public boolean watchApplierIfNotWatched(Long clusterDbId, Long shardDbId) {
		return currentMeta.watchApplierIfNotWatched(clusterDbId, shardDbId);
	}

	@Override
	public void setCurrentCRDTMaster(Long clusterDbId, Long shardDbId, long gid, String ip, int port) {
		RedisMeta currentMaster = new RedisMeta().setIp(ip).setPort(port).setGid(gid);
		currentMeta.setCurrentCRDTMaster(clusterDbId, shardDbId, currentMaster);
		notifyCurrentMasterChanged(clusterDbId, shardDbId);
	}

	@Override
	public RedisMeta getCurrentCRDTMaster(Long clusterDbId, Long shardDbId) {
		return currentMeta.getCurrentCRDTMaster(clusterDbId, shardDbId);
	}

	@Override
	public RedisMeta getCurrentMaster(Long clusterDbId, Long shardDbId) {
		List<KeeperMeta> keeperMetaList = dcMetaCache.getShardKeepers(clusterDbId, shardDbId);
		boolean hasKeeperMeta = keeperMetaList != null && !keeperMetaList.isEmpty();
		return currentMeta.getCurrentMaster(clusterDbId, shardDbId, hasKeeperMeta);
	}

	@Override
	public void setPeerMaster(String dcId, Long clusterDbId, Long shardDbId, long gid, String ip, int port) {
		if (dcMetaCache.getCurrentDc().equalsIgnoreCase(dcId)) {
			throw new IllegalArgumentException(String.format("peer master must from other dc %d %d %d %s:%d",
					clusterDbId, shardDbId, gid, ip, port));
		}

		RedisMeta peerMaster = new RedisMeta().setIp(ip).setPort(port).setGid(gid);
		currentMeta.setPeerMaster(dcId, clusterDbId, shardDbId, peerMaster);
		notifyPeerMasterChange(clusterDbId, shardDbId);
	}

	@Override
	public RedisMeta getPeerMaster(String dcId, Long clusterDbId, Long shardDbId) {
		return currentMeta.getPeerMaster(dcId, clusterDbId, shardDbId);
	}

	@Override
	public Set<String> getUpstreamPeerDcs(Long clusterDbId, Long shardDbId) {
		return currentMeta.getUpstreamPeerDcs(clusterDbId, shardDbId);
	}

	public Map<String, RedisMeta> getAllPeerMasters(Long clusterDbId, Long shardDbId) {
		return currentMeta.getAllPeerMasters(clusterDbId, shardDbId);
	}

	@Override
	public RouteMeta getClusterRouteByDcId(String dstDcId, Long clusterDbId) {
		return dcMetaCache.chooseRoute(clusterDbId, dstDcId);
	}

	@Override
	public void removePeerMaster(String dcId, Long clusterDbId, Long shardDbId) {
		currentMeta.removePeerMaster(dcId, clusterDbId, shardDbId);
	}

	private void notifyCurrentMasterChanged(Long clusterDbId, Long shardDbId) {
		for (MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.currentMasterChanged(clusterDbId, shardDbId);
			} catch (Exception e) {
				logger.error("[notifyCurrentMasterChanged] cluster_{}, shard_{}", clusterDbId, shardDbId, e);
			}
		}
	}

	@VisibleForTesting
	protected void notifyPeerMasterChange(Long clusterDbId, Long shardDbId) {
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.peerMasterChanged(clusterDbId, shardDbId);
			} catch (Exception e) {
				logger.error("[notifyPeerMasterChange] cluster_{}, shard_{}", clusterDbId, shardDbId, e);
			}
		}
	}

	private void notifyKeeperActiveElected(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper) {
		
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.keeperActiveElected(clusterDbId, shardDbId, activeKeeper);
			} catch (Exception e) {
				logger.error("[notifyKeeperActiveElected]cluster_" + clusterDbId + ",shard_" + shardDbId + "," + activeKeeper, e);
			}
		}
	}

	private void notifyApplierActiveElected(Long clusterDbId, Long shardDbId, ApplierMeta activeApplier, String sids) {

		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.applierActiveElected(clusterDbId, shardDbId, activeApplier, sids);
			} catch (Exception e) {
				logger.error("[notifyApplierActiveElected]cluster_" + clusterDbId + ",shard_" + shardDbId + "," + activeApplier + ",sids_" + sids, e);
			}
		}
	}

	private void notifyKeeperMasterChanged(Long clusterDbId, Long shardDbId, Pair<String, Integer> keeperMaster) {
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.keeperMasterChanged(clusterDbId, shardDbId, keeperMaster);
			} catch (Exception e) {
				logger.error("[notifyKeeperMasterChanged]cluster_" + clusterDbId + ",shard_" + shardDbId + "," + keeperMaster, e);
			}
		}
	}

	private void notifyApplierMasterChanged(Long clusterDbId, Long shardDbId, Pair<String, Integer> applierMaster, String sids) {
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.applierMasterChanged(clusterDbId, shardDbId, applierMaster, sids);
			} catch (Exception e) {
				logger.error("[notifyApplierMasterChanged]cluster_" + clusterDbId + ",shard_" + shardDbId + "," + applierMaster + ",sids_" + sids, e);
			}
		}
	}

	public void setSlotManager(SlotManager slotManager) {
		this.slotManager = slotManager;
	}
	
	public void setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
	}

	@VisibleForTesting
	protected void setCurrentMeta(CurrentMeta currentMeta) {
		this.currentMeta = currentMeta;
	}

	@VisibleForTesting
	protected void addMetaServerStateChangeHandler(MetaServerStateChangeHandler handler) {
		if(stateHandlers == null) {
			stateHandlers = Lists.newArrayList();
		}
		stateHandlers.add(handler);
	}
}
