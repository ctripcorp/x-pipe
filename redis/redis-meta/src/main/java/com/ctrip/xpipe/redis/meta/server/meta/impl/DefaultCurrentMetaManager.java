package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
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
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
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
		List<String> changedDcs = currentMeta.updateClusterRoutes(clusterMeta, dcMetaCache.getAllRoutes());
		if(changedDcs != null && !changedDcs.isEmpty())  {
			if(ClusterType.isSameClusterType(clusterMeta.getType(), ClusterType.BI_DIRECTION)) {
				for (ShardMeta shard : clusterMeta.getShards().values()) {
					changedDcs.forEach(dcId -> {
						notifyPeerMasterChange(dcId, clusterMeta.getDbId(), shard.getDbId());
					});
				}
			}
		}
	}

	private boolean isReCreateCluster(ClusterMeta current, ClusterMeta future) {
		//cluster type changed should recreate CurrentClusterMeta
		return !ObjectUtils.equals(current.getType(), future.getType());
	}
	
	private boolean needUpdateClusterRoutesWhenClusterChange(ClusterMeta current, ClusterMeta future) {
		ClusterType clusterType = ClusterType.lookup(future.getType());
		if(clusterType == null) {
			logger.error("[unknown cluster type] cluster id: {}, type :{}", future.getId(), future.getType());
			return false;
		}
		if(clusterType.supportMultiActiveDC()) {
			//arraylist join ","  
			return !ObjectUtils.equals(current.getDcs(), future.getDcs());
		} else {
			return !ObjectUtils.equals(current.getActiveDc(), future.getActiveDc());
		}
	}

	private void handleClusterChanged(ClusterMetaComparator clusterMetaComparator) {
		Long clusterDbId = clusterMetaComparator.getCurrent().getDbId();
		if(currentMeta.hasCluster(clusterDbId)){
			ClusterMeta current = clusterMetaComparator.getCurrent();
			ClusterMeta future = clusterMetaComparator.getFuture();
			if(isReCreateCluster(current, future)) {
				destroyCluster(current);
				addCluster(clusterDbId);
			} else {
				currentMeta.changeCluster(clusterMetaComparator);
				if(needUpdateClusterRoutesWhenClusterChange(current, future)) {
					clusterRoutesChange(clusterDbId);
				}
				notifyObservers(clusterMetaComparator);
			}

		}else{
			logger.warn("[handleClusterChanged][but we do not has it]{}", clusterMetaComparator);
			addCluster(clusterDbId);
		}
	}


	private synchronized void addCluster(Long clusterDbId) {
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
		List<RouteMeta> routes = dcMetaCache.getAllRoutes();
		currentMeta.updateClusterRoutes(clusterMeta, routes);
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
			ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterDbId);
			ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
			if(ClusterType.ONE_WAY.equals(clusterType)) {
				if(randomRoute(clusterDbId) != null) {
					refreshKeeperMaster(clusterMeta);
				}
			} else if(ClusterType.BI_DIRECTION.equals(clusterType)) {
				clusterRoutesChange(clusterMeta.getDbId());
			}
		}
	}

	@VisibleForTesting
	protected void refreshKeeperMaster(ClusterMeta clusterMeta) {
		Collection<ShardMeta> shards = clusterMeta.getShards().values();
		Long clusterDbId = clusterMeta.getDbId();
		for (ShardMeta shard : shards) {
			notifyKeeperMasterChanged(clusterDbId, shard.getDbId(), getKeeperMaster(clusterDbId, shard.getDbId()));
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
	public RouteMeta randomRoute(Long clusterDbId) {
		return dcMetaCache.randomRoute(clusterDbId);
	}


	@Override
	public Pair<String, Integer> getKeeperMaster(Long clusterDbId, Long shardDbId) {
		return currentMeta.getKeeperMaster(clusterDbId, shardDbId);
	}


	@Override
	public List<KeeperMeta> getSurviveKeepers(Long clusterDbId, Long shardDbId) {
		return currentMeta.getSurviveKeepers(clusterDbId, shardDbId);
	}

	@Override
	public KeeperMeta getKeeperActive(Long clusterDbId, Long shardDbId) {
		return currentMeta.getKeeperActive(clusterDbId, shardDbId);
	}
	
	@Override
	public String getCurrentMetaDesc() {
	
		Map<String, Object> desc = new HashMap<>();
		desc.put("meta", currentMeta);
		desc.put("currentSlots", currentSlots);
		JsonCodec codec = new JsonCodec(true, true);
		return codec.encode(desc);
	}
	
	protected Set<Integer> getCurrentSlots() {
		return currentSlots;
	}
	
	
	/*******************update dynamic info*************************/
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
	public void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviceKeepers, KeeperMeta activeKeeper) {
		currentMeta.setSurviveKeepers(clusterDbId, shardDbId, surviceKeepers, activeKeeper);
		notifyKeeperActiveElected(clusterDbId, shardDbId, activeKeeper);
	}

	@Override
	public void setKeeperMaster(Long clusterDbId, Long shardDbId, String ip, int port) {
		
		
		Pair<String, Integer> keeperMaster = new Pair<String, Integer>(ip, port);
		if(currentMeta.setKeeperMaster(clusterDbId, shardDbId, keeperMaster)){
			logger.info("[setKeeperMaster]cluster_{},shard_{},{}:{}", clusterDbId, shardDbId, ip, port);
			notifyKeeperMasterChanged(clusterDbId, shardDbId, keeperMaster);
		}else{
			logger.info("[setKeeperMaster][keeper master not changed!]cluster_{},shard_{},{}:{}", clusterDbId, shardDbId, ip, port);
		}
		
	}

	@Override
	public void setKeeperMaster(Long clusterDbId, Long shardDbId, String addr) {
		
		logger.info("[setKeeperMaster]cluster_{},shard_{},{}", clusterDbId, shardDbId, addr);
		Pair<String, Integer> inetAddr = IpUtils.parseSingleAsPair(addr);
		setKeeperMaster(clusterDbId, shardDbId, inetAddr.getKey(), inetAddr.getValue());
	}

	@Override
	public boolean watchIfNotWatched(Long clusterDbId, Long shardDbId) {
		return currentMeta.watchIfNotWatched(clusterDbId, shardDbId);
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
		return currentMeta.getCurrentMaster(clusterDbId, shardDbId);
	}

	@Override
	public void setPeerMaster(String dcId, Long clusterDbId, Long shardDbId, long gid, String ip, int port) {
		if (dcMetaCache.getCurrentDc().equalsIgnoreCase(dcId)) {
			throw new IllegalArgumentException(String.format("peer master must from other dc %d %d %d %s:%d",
					clusterDbId, shardDbId, gid, ip, port));
		}

		RedisMeta peerMaster = new RedisMeta().setIp(ip).setPort(port).setGid(gid);
		currentMeta.setPeerMaster(dcId, clusterDbId, shardDbId, peerMaster);
		notifyPeerMasterChange(dcId, clusterDbId, shardDbId);
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
	public RouteMeta getClusterRouteByDcId(String dcId, Long clusterDbId) {
		return currentMeta.getClusterRouteByDcId(clusterDbId, dcId);
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
	protected void notifyPeerMasterChange(String dcId, Long clusterDbId, Long shardDbId) {
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.peerMasterChanged(dcId, clusterDbId, shardDbId);
			} catch (Exception e) {
				logger.error("[notifyPeerMasterChange] {}, cluster_{}, shard_{}", dcId, clusterDbId, shardDbId, e);
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

	private void notifyKeeperMasterChanged(Long clusterDbId, Long shardDbId, Pair<String, Integer> keeperMaster) {
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.keeperMasterChanged(clusterDbId, shardDbId, keeperMaster);
			} catch (Exception e) {
				logger.error("[notifyKeeperMasterChanged]cluster_" + clusterDbId + ",shard_" + shardDbId + "," + keeperMaster, e);
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
