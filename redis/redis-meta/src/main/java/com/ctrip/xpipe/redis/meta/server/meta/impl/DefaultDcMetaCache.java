package com.ctrip.xpipe.redis.meta.server.meta.impl;


import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultDcMetaManager;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategy;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.ResourceAccessException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author wenchao.meng
 *
 *         Jul 7, 2016
 */
@Component
public class DefaultDcMetaCache extends AbstractLifecycleObservable implements DcMetaCache, Runnable, TopElement {

	public static final String META_MODIFY_JUST_NOW_TEMPLATE = "current dc meta modifyTime {}, loadTime {}";

	public static String MEMORY_META_SERVER_DAO_KEY = "memory_meta_server_dao_file";

	public static int META_MODIFY_PROTECT_COUNT = 20;

	public static final String META_CHANGE_TYPE = "MetaChange";

	private Map<Long, ReentrantLock> lockMap = Maps.newConcurrentMap();

	@Autowired(required = false)
	private ConsoleService consoleService;

	@Autowired
	private MetaServerConfig metaServerConfig;

	@Autowired
	private RouteChooseStrategyFactory routeChooseStrategyFactory;

	private RouteChooseStrategy strategy = null;

	private String currentDc = FoundationService.DEFAULT.getDataCenter();

	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("Meta-Refresher"));

	private ScheduledFuture<?> future;

	private AtomicReference<DcMetaManager> dcMetaManager = new AtomicReference<DcMetaManager>(null);

	private AtomicLong metaModifyTime = new AtomicLong(System.currentTimeMillis());

	public DefaultDcMetaCache() {
	}

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

		logger.info("[doInitialize][dc]{}", currentDc);
		this.dcMetaManager.set(loadMetaManager());
	}

	protected DcMetaManager loadMetaManager() {

		DcMetaManager dcMetaManager = null;
		if (consoleService != null) {
			try {
				logger.info("[loadMetaManager][load from console]");
				DcMeta dcMeta = loadMetaFromConsole();
				dcMetaManager = DefaultDcMetaManager.buildFromDcMeta(dcMeta);
			} catch (ResourceAccessException e) {
				logger.error("[loadMetaManager][consoleService]" + e.getMessage());
			} catch (Exception e) {
				logger.error("[loadMetaManager][consoleService]", e);
			}
		}

		if (dcMetaManager == null) {
			String fileName = System.getProperty(MEMORY_META_SERVER_DAO_KEY, "memory_meta_server_dao_file.xml");
			logger.info("[loadMetaManager][load from file]{}", fileName);
			dcMetaManager = DefaultDcMetaManager.buildFromFile(currentDc, fileName);
		}

		logger.info("[loadMetaManager]{}", dcMetaManager);

		if (dcMetaManager == null) {
			throw new IllegalArgumentException("[loadMetaManager][fail]");
		}
		return dcMetaManager;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();

		future = scheduled.scheduleAtFixedRate(this, 0, metaServerConfig.getMetaRefreshMilli(), TimeUnit.MILLISECONDS);

	}

	@Override
	protected void doStop() throws Exception {

		future.cancel(true);
		super.doStop();
	}

	@Override
	public void run() {

		try {
			if (consoleService != null) {
				long metaLoadTime = System.currentTimeMillis();
				DcMeta future = loadMetaFromConsole();
				DcMeta current = dcMetaManager.get().getDcMeta();

				changeDcMeta(current, future, metaLoadTime);
				checkRouteChange(current, future);
			}
		} catch (Throwable th) {
			logger.error("[run]" + th.getMessage());
		}
	}

	private DcMeta loadMetaFromConsole() {
		Set<String> types = metaServerConfig.getOwnClusterType();
		return consoleService.getDcMeta(currentDc, types);
	}

	@VisibleForTesting
	protected void changeDcMeta(DcMeta current, DcMeta future, final long metaLoadTime) {

		if (!mayMetaUpdateFromConsole(metaLoadTime)) {
			logger.info("[run][skip change dc meta]" + META_MODIFY_JUST_NOW_TEMPLATE, metaModifyTime.get(), metaLoadTime);
			return;
		}

		DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
		dcMetaComparator.setShardMigrateSupport();
		dcMetaComparator.compare();

		if (dcMetaComparator.totalChangedCount() - drClusterNums(dcMetaComparator) - keeperMigrateOnlyNums(dcMetaComparator) > META_MODIFY_PROTECT_COUNT) {
			logger.error("[run][modify count size too big]{}, {}, {}", META_MODIFY_PROTECT_COUNT,
					dcMetaComparator.totalChangedCount(), dcMetaComparator);
			EventMonitor.DEFAULT.logAlertEvent("remove too many:" + dcMetaComparator.totalChangedCount());
			return;
		}

		DcMetaManager newDcMetaManager = DefaultDcMetaManager.buildFromDcMeta(future);
		boolean dcMetaUpdated = false;
		synchronized (this) {
			if (mayMetaUpdateFromConsole(metaLoadTime)) {
				dcMetaManager.set(newDcMetaManager);
				dcMetaUpdated = true;
			}
		}

		if (!dcMetaUpdated) {
			logger.info("[run][skip change dc meta]" + META_MODIFY_JUST_NOW_TEMPLATE, metaModifyTime, metaLoadTime);
			return;
		}

		logger.info("[run][change dc meta]");
		if (dcMetaComparator.totalChangedCount() > 0) {
			logger.info("[run][change]{}", dcMetaComparator);
			EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("[add:%s, del:%s, mod:%s]",
					StringUtil.join(",", (clusterMeta) -> clusterMeta.getId(), dcMetaComparator.getAdded()),
					StringUtil.join(",", (clusterMeta) -> clusterMeta.getId(), dcMetaComparator.getRemoved()),
					StringUtil.join(",", (comparator) -> comparator.idDesc(), dcMetaComparator.getMofified()))
			);
			notifyObservers(dcMetaComparator);
		}
	}

	private int drClusterNums(DcMetaComparator comparator) {
		int result = 0;
		for(MetaComparator metaComparator : comparator.getMofified()) {
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) metaComparator;
			DrMigrationChecker checker = new DrMigrationChecker(clusterMetaComparator);
			if(checker.isDrMigrationChange()) {
				EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("[migrate: %s]", clusterMetaComparator.getCurrent().getId()));
				result ++;
			}
		}
		logger.info("[DR Switched][cluster num] {}", result);
		return result;
	}

	@VisibleForTesting
	protected int keeperMigrateOnlyNums(DcMetaComparator comparator) {
		int keeperMigOnlyCnt = 0;
		ClusterCompare: for (MetaComparator metaComparator : comparator.getMofified()) {
			ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) metaComparator;
			if (clusterMetaComparator.isConfigChange() || clusterMetaComparator.getMofified().isEmpty()
					|| !clusterMetaComparator.getAdded().isEmpty() || !clusterMetaComparator.getRemoved().isEmpty()) continue;

			for (MetaComparator innerMetaComparator : clusterMetaComparator.getMofified()) {
				ShardMetaComparator shardMetaComparator = (ShardMetaComparator) innerMetaComparator;
				if (shardMetaComparator.isConfigChange() || !shardMetaComparator.getMofified().isEmpty()) continue ClusterCompare;
				if (shardMetaComparator.getAdded().isEmpty() && shardMetaComparator.getRemoved().isEmpty()) continue ClusterCompare;
				if (shardMetaComparator.getAdded().stream().anyMatch(node -> !(node instanceof KeeperMeta))) continue ClusterCompare;
				if (shardMetaComparator.getRemoved().stream().anyMatch(node -> !(node instanceof KeeperMeta))) continue ClusterCompare;
			}
			EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, "KeeperMigrate");
			keeperMigOnlyCnt++;
		}
		logger.info("[keeperMigrateOnlyNums] {}", keeperMigOnlyCnt);
		return keeperMigOnlyCnt;
	}

	@VisibleForTesting
	protected void checkRouteChange(DcMeta current, DcMeta future) {
		DcRouteMetaComparator comparator = new DcRouteMetaComparator(current, future, Route.TAG_META);
		comparator.compare();

		if(!comparator.getRemoved().isEmpty()
				|| !comparator.getMofified().isEmpty()) {
			logger.info("[run][route][change]{}", comparator);
			notifyObservers(comparator);
		}
	}

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

	public DcMetaManager getDcMeta() {
		return this.dcMetaManager.get();
	}

	@Override
	public String clusterDbId2Name(Long clusterDbId) {
		return dcMetaManager.get().clusterDbId2Name(clusterDbId);
	}

	@Override
	public Pair<String, String> clusterShardDbId2Name(Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().clusterShardDbId2Name(clusterDbId, shardDbId);
	}

	@Override
	public Long clusterId2DbId(String clusterId) {
		return dcMetaManager.get().clusterId2DbId(clusterId);
	}

	@Override
	public Pair<Long, Long> clusterShardId2DbId(String clusterId, String shardId) {
		return dcMetaManager.get().clusterShardId2DbId(clusterId, shardId);
	}

	@Override
	public Set<ClusterMeta> getClusters() {
		return dcMetaManager.get().getClusters();
	}

	@Override
	public ClusterMeta getClusterMeta(Long clusterDbId) {
		return dcMetaManager.get().getClusterMeta(clusterDbId);
	}

	@Override
	public ClusterType getClusterType(Long clusterDbId) {
		return dcMetaManager.get().getClusterType(clusterDbId);
	}

	@Override
	public List<RouteMeta> getAllMetaRoutes() {
		return dcMetaManager.get().getAllMetaRoutes();
	}

	@Override
	public RouteMeta chooseRoute(long clusterDbId, String dstDcId) {
		ClusterMeta clusterMeta = getClusterMeta(clusterDbId);
		RouteChooseStrategyFactory.RouteStrategyType routeStrategyType =
				RouteChooseStrategyFactory.RouteStrategyType.lookup(metaServerConfig.getChooseRouteStrategyType());
		return dcMetaManager.get().chooseRoute(clusterMeta, dstDcId, getRouteChooseStrategy(routeStrategyType));
	}

	private RouteChooseStrategy getRouteChooseStrategy(RouteChooseStrategyFactory.RouteStrategyType routeStrategyType) {
		RouteChooseStrategy localStrategy = strategy;
		if(null == localStrategy || !ObjectUtils.equals(routeStrategyType, localStrategy.getRouteStrategyType())) {
			localStrategy = routeChooseStrategyFactory.create(routeStrategyType);
			strategy = localStrategy;
		}

		return localStrategy;
	}

	@Override
	public KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta) {
		return dcMetaManager.get().getKeeperContainer(keeperMeta);
	}

	@Override
	public ApplierContainerMeta getApplierContainer(ApplierMeta applierMeta) {
	    return dcMetaManager.get().getApplierContainer(applierMeta);
	}

	@Override
	public void clusterAdded(ClusterMeta clusterMeta) {

		EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("add:%s", clusterMeta.getId()));

		clusterModified(clusterMeta);
	}

	@Override
	public void clusterModified(ClusterMeta clusterMeta) {

		EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("mod:%s", clusterMeta.getId()));

		ReentrantLock lock = lockMap.computeIfAbsent(clusterMeta.getDbId(), key -> new ReentrantLock());

		ClusterMeta current = null;
		try {
			lock.lock();
			current = dcMetaManager.get().getClusterMeta(clusterMeta.getId());
			dcMetaManager.get().update(clusterMeta);
			updateMetaLastChangeTime();
		} catch (Exception e) {
			logger.info("[clusterModified]exception{}, {}", current, clusterMeta, e);
			throw e;
		} finally {
			lock.unlock();
		}

		logger.info("[clusterModified]{}, {}", current, clusterMeta);
		DcMetaComparator dcMetaComparator = DcMetaComparator.buildClusterChanged(current, clusterMeta);
		notifyObservers(dcMetaComparator);
	}

	private void updateMetaLastChangeTime() {
		synchronized (this) {
			metaModifyTime.set(System.currentTimeMillis());
		}
	}

	@Override
	public void clusterDeleted(Long clusterDbId) {
		ClusterMeta clusterMeta = dcMetaManager.get().getClusterMeta(clusterDbId);
		if (clusterMeta == null) {
			logger.warn("[clusterDeleted][already deleted]{}", clusterDbId);
			return;
		}

		EventMonitor.DEFAULT.logEvent(META_CHANGE_TYPE, String.format("del:%d", clusterDbId));
		clusterMeta = dcMetaManager.get().removeCluster(clusterDbId);
		lockMap.remove(clusterDbId);

		logger.info("[clusterDeleted]{}", clusterMeta);
		DcMetaComparator dcMetaComparator = DcMetaComparator.buildClusterRemoved(clusterMeta);
		notifyObservers(dcMetaComparator);
	}

	@Override
	public String getCurrentDc() {
		return currentDc;
	}

	@Override
	public boolean isCurrentDcPrimary(Long clusterDbId, Long shardDbId) {
		return currentDc.equalsIgnoreCase(dcMetaManager.get().getActiveDc(clusterDbId, shardDbId));
	}
	
	@Override
	public boolean isCurrentDcPrimary(Long clusterDbId) {
		return isCurrentDcPrimary(clusterDbId, null);
	}

	@Override
	public boolean isCurrentDcBackUp(Long clusterDbId, Long shardDbId) {
	    Set<String> dcSet = dcMetaManager.get().getBackupDcs(clusterDbId, shardDbId);
		if (CollectionUtils.isEmpty(dcSet)) {
			return false;
		}

		return dcSet.contains(currentDc.toLowerCase());
	}

	@Override
	public boolean isCurrentDcBackUp(Long clusterDbId) {
		return isCurrentDcBackUp(clusterDbId, null);
	}

	@Override
	public boolean isCurrentShardParentCluster(Long clusterDbId, Long shardDbId) {
	    Pair<String, String> clusterShardDbId2Name = clusterShardDbId2Name(clusterDbId, shardDbId);
		return dcMetaManager.get().getClusterMeta(clusterDbId).getShards().containsKey(clusterShardDbId2Name.getValue());
	}

	@Override
	public List<KeeperMeta> getShardKeepers(Long clusterDbId, Long shardDbId) {
			return dcMetaManager.get().getKeepers(clusterDbId, shardDbId);
	}

	@Override
	public List<ApplierMeta> getShardAppliers(Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().getAppliers(clusterDbId, shardDbId);
	}

	@Override
	public List<RedisMeta> getClusterRedises(Long clusterDbId) {
	    return dcMetaManager.get().getRedises(clusterDbId);
	}

	@Override
	public List<RedisMeta> getShardRedises(Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().getRedises(clusterDbId, shardDbId);
	}

	@Override
	public Set<String> getBakupDcs(Long clusterDbId, Long shardDbId) {
		
		return dcMetaManager.get().getBackupDcs(clusterDbId, shardDbId);
	}

	@Override
	public Set<String> getDownstreamDcs(String dc, Long clusterDbId, Long shardDbId) {

		return dcMetaManager.get().getDownstreamDcs(dc, clusterDbId, shardDbId);
	}

	@Override
	public String getUpstreamDc(String dc, Long clusterDbId, Long shardDbId) {
	    return dcMetaManager.get().getUpstreamDc(dc, clusterDbId, shardDbId);
	}

	@Override
	public String getSrcDc(String dc, Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().getSrcDc(dc, clusterDbId, shardDbId);
	}

	@Override
	public Set<String> getRelatedDcs(Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().getRelatedDcs(clusterDbId, shardDbId);
	}

	@Override
	public String getPrimaryDc(Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().getActiveDc(clusterDbId, shardDbId);
	}

	@Override
	public SentinelMeta getSentinel(Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().getSentinel(clusterDbId, shardDbId);
	}

	@Override
	public String getSentinelMonitorName(Long clusterDbId, Long shardDbId) {
		return dcMetaManager.get().getSentinelMonitorName(clusterDbId, shardDbId);
	}

	@Override
	public void primaryDcChanged(Long clusterDbId, Long shardDbId, String newPrimaryDc) {
		updateMetaLastChangeTime();
		dcMetaManager.get().primaryDcChanged(clusterDbId, shardDbId, newPrimaryDc);
	}

	private boolean mayMetaUpdateFromConsole(final long metaLoadTime) {
		// consider that meta from console may be old because of db sync delay or other
		return metaLoadTime > metaModifyTime.get() + metaServerConfig.getWaitForMetaSyncDelayMilli();
	}


	/** we believe a cluster meta change comes from DR migration under these circumstances:
	 * 1. cluster meta comparator contains no shard add/delete
	 * 2. shard meta comparator contains no redis/keeper add/delete
	 * */
	private class DrMigrationChecker {

		private ClusterMetaComparator comparator;

		public DrMigrationChecker(ClusterMetaComparator comparator) {
			this.comparator = comparator;
		}

		public boolean isDrMigrationChange() {
			if(!comparator.getAdded().isEmpty() || !comparator.getRemoved().isEmpty()) {
				return false;
			}
			for(MetaComparator metaComparator : comparator.getMofified()) {
				ShardMetaComparator shardMetaComparator = (ShardMetaComparator) metaComparator;
				if(!isShardChangedByDrOnly(shardMetaComparator)) {
					return false;
				}
 			}
 			return true;
		}

		private boolean isShardChangedByDrOnly(ShardMetaComparator shardMetaComparator) {
			return shardMetaComparator.getAdded().isEmpty() && shardMetaComparator.getRemoved().isEmpty();
		}
	}

	@VisibleForTesting
	protected void setMetaServerConfig(MetaServerConfig metaServerConfig) {
		this.metaServerConfig = metaServerConfig;
	}

	@VisibleForTesting
	protected void setRouteChooseStrategyFactory(RouteChooseStrategyFactory routeChooseStrategyFactory) {
		this.routeChooseStrategyFactory = routeChooseStrategyFactory;
	}

	@VisibleForTesting
	protected AtomicLong getMetaModifyTime() {
		return metaModifyTime;
	}
}
