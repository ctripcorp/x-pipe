package com.ctrip.xpipe.redis.meta.server.meta.impl;


import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultDcMetaManager;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 *         Jul 7, 2016
 */
@Component
public class DefaultDcMetaCache extends AbstractLifecycleObservable implements DcMetaCache, Runnable, TopElement {

	public static String MEMORY_META_SERVER_DAO_KEY = "memory_meta_server_dao_file";

	public static int META_DELETE_PROTECT_COUNT = 10;

	@Autowired(required = false)
	private ConsoleService consoleService;

	@Autowired
	private MetaServerConfig metaServerConfig;

	private String currentDc = FoundationService.DEFAULT.getDataCenter();

	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("Meta-Refresher"));

	private ScheduledFuture<?> future;

	private AtomicReference<DcMetaManager> dcMetaManager = new AtomicReference<DcMetaManager>(null);

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
				DcMeta dcMeta = consoleService.getDcMeta(currentDc);
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

				DcMeta future = consoleService.getDcMeta(currentDc);
				DcMeta current = dcMetaManager.get().getDcMeta();

				DcMetaComparator dcMetaComparator = new DcMetaComparator(current, future);
				dcMetaComparator.compare();

				if (dcMetaComparator.getRemoved().size() > META_DELETE_PROTECT_COUNT) {
					logger.error("[run][removed count size too big]{}", META_DELETE_PROTECT_COUNT,
							dcMetaComparator.getRemoved());
					return;
				}

				logger.info("[run][change dc meta]");
				dcMetaManager.set(DefaultDcMetaManager.buildFromDcMeta(future));
				if (dcMetaComparator.totalChangedCount() > 0) {
					logger.info("[run][change]{}", dcMetaComparator);
					notifyObservers(dcMetaComparator);
				}
			}
		} catch (Throwable th) {
			logger.error("[run]" + th.getMessage());
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
	public Set<String> getClusters() {
		return dcMetaManager.get().getClusters();
	}

	@Override
	public ClusterMeta getClusterMeta(String clusterId) {
		return dcMetaManager.get().getClusterMeta(clusterId);
	}

	@Override
	public KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta) {
		return dcMetaManager.get().getKeeperContainer(keeperMeta);
	}

	public void clusterAdded(ClusterMeta clusterMeta) {
		clusterModified(clusterMeta);
	}

	public void clusterModified(ClusterMeta clusterMeta) {

		ClusterMeta current = dcMetaManager.get().getClusterMeta(clusterMeta.getId());
		dcMetaManager.get().update(clusterMeta);

		logger.info("[clusterModified]{}, {}", current, clusterMeta);
		DcMetaComparator dcMetaComparator = DcMetaComparator.buildClusterChanged(current, clusterMeta);
		notifyObservers(dcMetaComparator);
	}

	public void clusterDeleted(String clusterId) {

		ClusterMeta clusterMeta = dcMetaManager.get().removeCluster(clusterId);
		logger.info("[clusterDeleted]{}", clusterMeta);
		DcMetaComparator dcMetaComparator = DcMetaComparator.buildClusterRemoved(clusterMeta);
		notifyObservers(dcMetaComparator);
	}

	@Override
	public String getCurrentDc() {
		return currentDc;
	}

	@Override
	public boolean isCurrentDcPrimary(String clusterId, String shardId) {
		return currentDc.equalsIgnoreCase(dcMetaManager.get().getActiveDc(clusterId, shardId));
	}
	
	@Override
	public boolean isCurrentDcPrimary(String clusterId) {
		return isCurrentDcPrimary(clusterId, null);
	}


	@Override
	public List<KeeperMeta> getShardKeepers(String clusterId, String shardId) {
		return dcMetaManager.get().getKeepers(clusterId, shardId);
	}

	@Override
	public List<RedisMeta> getShardRedises(String clusterId, String shardId) {
		return dcMetaManager.get().getRedises(clusterId, shardId);
	}

	@Override
	public Set<String> getBakupDcs(String clusterId, String shardId) {
		
		return dcMetaManager.get().getBackupDcs(clusterId, shardId);
	}

	@Override
	public String getPrimaryDc(String clusterId, String shardId) {
		return dcMetaManager.get().getActiveDc(clusterId, shardId);
	}

	@Override
	public SentinelMeta getSentinel(String clusterId, String shardId) {
		return dcMetaManager.get().getSentinel(clusterId, shardId);
	}

	@Override
	public String getSentinelMonitorName(String clusterId, String shardId) {
		return dcMetaManager.get().getSentinelMonitorName(clusterId, shardId);
	}

	@Override
	public void primaryDcChanged(String clusterId, String shardId, String newPrimaryDc) {
		dcMetaManager.get().primaryDcChanged(clusterId, shardId, newPrimaryDc);
	}


}
