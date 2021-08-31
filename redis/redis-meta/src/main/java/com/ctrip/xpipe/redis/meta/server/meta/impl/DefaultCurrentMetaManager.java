package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
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

	private void clusterRoutesChange(String clusterId) {
		ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterId);
		List<String> changedDcs = currentMeta.updateClusterRoutes(clusterMeta, dcMetaCache.getAllRoutes());
		if(changedDcs != null && !changedDcs.isEmpty())  {
			if(clusterMeta.getType().equals(ClusterType.BI_DIRECTION.name())) {
				Set<String> shards = clusterMeta.getShards().keySet();
				for (String shardId : shards) {
					changedDcs.forEach(dcId -> {
						notifyPeerMasterChange(dcId, clusterId, shardId);
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
		String clusterId = clusterMetaComparator.getCurrent().getId();
		if(currentMeta.hasCluster(clusterId)){
			ClusterMeta current = clusterMetaComparator.getCurrent();
			ClusterMeta future = clusterMetaComparator.getFuture();
			if(isReCreateCluster(current, future)) {
				destroyCluster(current);
				addCluster(clusterId);
			} else {
				currentMeta.changeCluster(clusterMetaComparator);
				if(needUpdateClusterRoutesWhenClusterChange(current, future)) {
					clusterRoutesChange(clusterId);
				}
				notifyObservers(clusterMetaComparator);
			}

		}else{
			logger.warn("[handleClusterChanged][but we do not has it]{}", clusterMetaComparator);
			addCluster(clusterId);
		}
	}


	private void addCluster(String clusterId) {

		ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterId);

		logger.info("[addCluster]{}, {}", clusterId, clusterMeta);
		currentMeta.addCluster(clusterMeta);
		List<RouteMeta> routes = dcMetaCache.getAllRoutes();
		currentMeta.updateClusterRoutes(clusterMeta, routes);
		notifyObservers(new NodeAdded<ClusterMeta>(clusterMeta));
	}

	private void destroyCluster(ClusterMeta clusterMeta){
		//keeper in clustermeta, keepermanager remove keeper
		removeCluster(clusterMeta);
	}
	
	private void removeCluster(ClusterMeta clusterMeta) {
		
		logger.info("[removeCluster]{}", clusterMeta.getId());
		boolean result = currentMeta.removeCluster(clusterMeta.getId()) != null;
		if(result){
			notifyObservers(new NodeDeleted<ClusterMeta>(clusterMeta));
		}
	}



	private void removeClusterInterested(String clusterId) {
		//notice
		removeCluster(new ClusterMeta(clusterId).setType(dcMetaCache.getClusterType(clusterId).toString()));
	}

	@Override
	public Set<String> allClusters() {
		return new HashSet<>(currentMeta.allClusters());
	}

	@Override
	public void deleteSlot(int slotId) {
		
		currentSlots.remove(slotId);
		logger.info("[deleteSlot]{}", slotId);
		for(String clusterId : new HashSet<>(currentMeta.allClusters())){
			
			int currentSlotId = slotManager.getSlotIdByKey(clusterId);
			if(currentSlotId == slotId){
				removeClusterInterested(clusterId);
			}
		}
	}


	@Override
	public void addSlot(int slotId) {
		
		logger.info("[addSlot]{}", slotId);
		currentSlots.add(slotId);
		for(String clusterId : dcMetaCache.getClusters()){
			
			int currentSlotId = slotManager.getSlotIdByKey(clusterId);
			if(currentSlotId == slotId){
				addCluster(clusterId);
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
			if(currentClusterServer.hasKey(clusterMeta.getId())){
				addCluster(clusterMeta.getId());
			}else{
				logger.info("[dcMetaChange][add][not interested]{}", clusterMeta.getId());
			}
		}
		
		for(ClusterMeta clusterMeta : comparator.getRemoved()){
			if(currentClusterServer.hasKey(clusterMeta.getId())){
				destroyCluster(clusterMeta);
			}else{
				logger.info("[dcMetaChange][destroy][not interested]{}", clusterMeta.getId());
			}

		}
		
		for(@SuppressWarnings("rawtypes") MetaComparator changedComparator : comparator.getMofified()){
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) changedComparator;
			String clusterId = clusterMetaComparator.getCurrent().getId();
			if(currentClusterServer.hasKey(clusterId)){
				handleClusterChanged(clusterMetaComparator);
			}else{
				logger.info("[dcMetaChange][change][not interested]{}", clusterId);
			}
		}
	}

	@VisibleForTesting
	protected void routeChanges() {
		for(String clusterId : allClusters()) {
			ClusterMeta clusterMeta = dcMetaCache.getClusterMeta(clusterId);
			String clusterType =clusterMeta.getType();
			if(clusterType.equalsIgnoreCase(ClusterType.ONE_WAY.name())) {
				if(randomRoute(clusterId) != null) {
					refreshKeeperMaster(clusterMeta);
				}
			} else if(clusterType.equalsIgnoreCase(ClusterType.BI_DIRECTION.name())) {
				clusterRoutesChange(clusterId);
			}
		}
	}

	@VisibleForTesting
	protected void refreshKeeperMaster(ClusterMeta clusterMeta) {
		Set<String> shards = clusterMeta.getShards().keySet();
		String clusterId = clusterMeta.getId();
		for (String shardId : shards) {
			notifyKeeperMasterChanged(clusterId, shardId, getKeeperMaster(clusterId, shardId));
		}
	}

	
	@Override
	public boolean hasCluster(String clusterId) {
		return currentMeta.hasCluster(clusterId);
	}

	@Override
	public boolean hasShard(String clusterId, String shardId) {
		return currentMeta.hasShard(clusterId, shardId);
	}
	
	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		return ((DefaultDcMetaCache)dcMetaCache).getDcMeta().getRedisMaster(clusterId, shardId);
	}

	@Override
	public ClusterMeta getClusterMeta(String clusterId) {
		return dcMetaCache.getClusterMeta(clusterId);
	}

	@Override
	public RouteMeta randomRoute(String clusterId) {
		return dcMetaCache.randomRoute(clusterId);
	}


	@Override
	public Pair<String, Integer> getKeeperMaster(String clusterId, String shardId) {
		return currentMeta.getKeeperMaster(clusterId, shardId);
	}


	@Override
	public List<KeeperMeta> getSurviveKeepers(String clusterId, String shardId) {
		return currentMeta.getSurviveKeepers(clusterId, shardId);
	}

	@Override
	public KeeperMeta getKeeperActive(String clusterId, String shardId) {
		return currentMeta.getKeeperActive(clusterId, shardId);
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
	public boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper) {
		boolean result = currentMeta.setKeeperActive(clusterId, shardId, activeKeeper);
		notifyKeeperActiveElected(clusterId, shardId, activeKeeper);
		return result;
	}
	
	@Override
	public void addResource(String clusterId, String shardId, Releasable releasable) {
		currentMeta.addResource(clusterId, shardId, releasable);
	}


	@Override
	public void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviceKeepers, KeeperMeta activeKeeper) {
		currentMeta.setSurviveKeepers(clusterId, shardId, surviceKeepers, activeKeeper);
		notifyKeeperActiveElected(clusterId, shardId, activeKeeper);
	}

	@Override
	public void setKeeperMaster(String clusterId, String shardId, String ip, int port) {
		
		
		Pair<String, Integer> keeperMaster = new Pair<String, Integer>(ip, port);
		if(currentMeta.setKeeperMaster(clusterId, shardId, keeperMaster)){
			logger.info("[setKeeperMaster]{},{},{}:{}", clusterId, shardId, ip, port);
			notifyKeeperMasterChanged(clusterId, shardId, keeperMaster);
		}else{
			logger.info("[setKeeperMaster][keeper master not changed!]{},{},{}:{}", clusterId, shardId, ip, port);
		}
		
	}

	@Override
	public void setKeeperMaster(String clusterId, String shardId, String addr) {
		
		logger.info("[setKeeperMaster]{},{},{}", clusterId, shardId, addr);
		Pair<String, Integer> inetAddr = IpUtils.parseSingleAsPair(addr);
		setKeeperMaster(clusterId, shardId, inetAddr.getKey(), inetAddr.getValue());
	}

	@Override
	public boolean watchIfNotWatched(String clusterId, String shardId) {
		return currentMeta.watchIfNotWatched(clusterId, shardId);
	}

	@Override
	public void setCurrentCRDTMaster(String clusterId, String shardId, long gid, String ip, int port) {
		RedisMeta currentMaster = new RedisMeta().setIp(ip).setPort(port).setGid(gid);
		currentMeta.setCurrentCRDTMaster(clusterId, shardId, currentMaster);
		notifyCurrentMasterChanged(clusterId, shardId);
	}

	@Override
	public RedisMeta getCurrentCRDTMaster(String clusterId, String shardId) {
		return currentMeta.getCurrentCRDTMaster(clusterId, shardId);
	}

	@Override
	public RedisMeta getCurrentMaster(String clusterId, String shardId) {
		return currentMeta.getCurrentMaster(clusterId, shardId);
	}

	@Override
	public void setPeerMaster(String dcId, String clusterId, String shardId, long gid, String ip, int port) {
		if (dcMetaCache.getCurrentDc().equalsIgnoreCase(dcId)) {
			throw new IllegalArgumentException(String.format("peer master must from other dc %s %s %d %s:%d",
					clusterId, shardId, gid, ip, port));
		}

		RedisMeta peerMaster = new RedisMeta().setIp(ip).setPort(port).setGid(gid);
		currentMeta.setPeerMaster(dcId, clusterId, shardId, peerMaster);
		notifyPeerMasterChange(dcId, clusterId, shardId);
	}

	@Override
	public RedisMeta getPeerMaster(String dcId, String clusterId, String shardId) {
		return currentMeta.getPeerMaster(dcId, clusterId, shardId);
	}

	@Override
	public Set<String> getUpstreamPeerDcs(String clusterId, String shardId) {
		return currentMeta.getUpstreamPeerDcs(clusterId, shardId);
	}

	public Map<String, RedisMeta> getAllPeerMasters(String clusterId, String shardId) {
		return currentMeta.getAllPeerMasters(clusterId, shardId);
	}

	@Override
	public RouteMeta getClusterRouteByDcId(String clusterId, String dcId) {
		return currentMeta.getClusterRouteByDcId(clusterId, dcId);
	}

	@Override
	public void removePeerMaster(String dcId, String clusterId, String shardId) {
		currentMeta.removePeerMaster(dcId, clusterId, shardId);
	}

	private void notifyCurrentMasterChanged(String clusterId, String shardId) {
		for (MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.currentMasterChanged(clusterId, shardId);
			} catch (Exception e) {
				logger.error("[notifyCurrentMasterChanged] {}, {}", clusterId, shardId, e);
			}
		}
	}

	private void notifyPeerMasterChange(String dcId, String clusterId, String shardId) {
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.peerMasterChanged(dcId, clusterId, shardId);
			} catch (Exception e) {
				logger.error("[notifyPeerMasterChange] {}, {}, {}", dcId, clusterId, shardId, e);
			}
		}
	}

	private void notifyKeeperActiveElected(String clusterId, String shardId, KeeperMeta activeKeeper) {
		
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.keeperActiveElected(clusterId, shardId, activeKeeper);
			} catch (Exception e) {
				logger.error("[notifyKeeperActiveElected]" + clusterId + "," + shardId + "," + activeKeeper, e);
			}
		}
	}

	private void notifyKeeperMasterChanged(String clusterId, String shardId, Pair<String, Integer> keeperMaster) {
		for(MetaServerStateChangeHandler stateHandler : stateHandlers){
			try {
				stateHandler.keeperMasterChanged(clusterId, shardId, keeperMaster);
			} catch (Exception e) {
				logger.error("[notifyKeeperMasterChanged]" + clusterId + "," + shardId + "," + keeperMaster, e);
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
