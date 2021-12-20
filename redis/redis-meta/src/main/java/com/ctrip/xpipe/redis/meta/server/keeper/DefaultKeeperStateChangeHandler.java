package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.command.ConditionalCommand;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServerStateChangeHandler;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.job.DefaultSlaveOfJob;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 *         Jul 8, 2016
 */
@Component
public class DefaultKeeperStateChangeHandler extends AbstractLifecycle implements MetaServerStateChangeHandler, TopElement {

	protected static Logger logger = LoggerFactory.getLogger(DefaultKeeperStateChangeHandler.class);

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	private ExecutorService executors;

	private KeyedOneThreadTaskExecutor<Pair<Long, Long>> keyedOneThreadTaskExecutor;

	@Autowired
	private DcMetaCache dcMetaCache;

	@Autowired
	private CurrentMetaManager currentMetaManager;

	@Autowired
	private MetaServerConfig config;

	private Map<Pair<Long, Long>, Long> lastKeeperAdjustTimeMap = Maps.newConcurrentMap();
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		executors = DefaultExecutorFactory.createAllowCoreTimeout("KeeperStateChangeHandler", OsUtils.defaultMaxCoreThreadCount()).createExecutorService();
		keyedOneThreadTaskExecutor = new KeyedOneThreadTaskExecutor<>(executors);
	}

	private boolean shouldSkipAdjust(Long clusterDbId, Long shardDbId) {
		Pair<Long, Long> clusterShardKey = Pair.of(clusterDbId, shardDbId);
		int minAdjustInterval = config.getMinKeeperAdjustIntervalMilli();
		Long lastAdjustTime = lastKeeperAdjustTimeMap.get(clusterShardKey);

		if (minAdjustInterval > 0 && null != lastAdjustTime && (System.currentTimeMillis() - lastAdjustTime) < minAdjustInterval) {
			logger.info("[skip adjust][last:{},minInterval:{}] cluster_{},shard_{}", lastAdjustTime, minAdjustInterval, clusterDbId, shardDbId);
			return true;
		} else return false;
	}

	private void updateAdjustTime(Long clusterDbId, Long shardDbId) {
		lastKeeperAdjustTimeMap.put(Pair.of(clusterDbId, shardDbId), System.currentTimeMillis());
	}
	
	@Override
	public void keeperMasterChanged(Long clusterDbId, Long shardDbId, Pair<String, Integer> newMaster) {
		if (shouldSkipAdjust(clusterDbId, shardDbId)) {
			logger.info("[keeperMasterChanged][skip]{},{},{}", clusterDbId, shardDbId, newMaster);
			return;
		}

		logger.info("[keeperMasterChanged]{},{},{}", clusterDbId, shardDbId, newMaster);
		updateAdjustTime(clusterDbId, shardDbId);
		KeeperMeta activeKeeper = currentMetaManager.getKeeperActive(clusterDbId, shardDbId);

		if (activeKeeper == null) {
			logger.info("[keeperMasterChanged][no active keeper, do nothing]{},{},{}", clusterDbId, shardDbId, newMaster);
			return;
		}
		if (!activeKeeper.isActive()) {
			throw new IllegalStateException("[active keeper not active]{}" + activeKeeper);
		}

		logger.info("[keeperMasterChanged][set active keeper master]{}, {}", activeKeeper, newMaster);

		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(activeKeeper);
		
		keyedOneThreadTaskExecutor.execute(
				new Pair<>(clusterDbId, shardDbId),
				createKeeperStateChangeJob(clusterDbId, keepers, newMaster));
	}

	private KeeperStateChangeJob createKeeperStateChangeJob(Long clusterDbId, List<KeeperMeta> keepers, Pair<String, Integer> master) {

		RouteMeta routeMeta = currentMetaManager.randomRoute(clusterDbId);
		return new KeeperStateChangeJob(keepers, master, routeMeta, clientPool, scheduled, executors);
	}

	@Override
	public void keeperActiveElected(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper) {
		if (shouldSkipAdjust(clusterDbId, shardDbId)) {
			logger.info("[keeperActiveElected][skip]{},{},{}", clusterDbId, shardDbId, activeKeeper);
			return;
		}

		logger.info("[keeperActiveElected]{},{},{}", clusterDbId, shardDbId, activeKeeper);
		updateAdjustTime(clusterDbId, shardDbId);

		List<KeeperMeta> keepers = currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId);
		if (keepers == null || keepers.isEmpty()) {
			logger.info("[keeperActiveElected][none keeper survive, do nothing]");
			return;
		}
		Pair<String, Integer> activeKeeperMaster = currentMetaManager.getKeeperMaster(clusterDbId, shardDbId);

		KeeperStateChangeJob keeperStateChangeJob = createKeeperStateChangeJob(clusterDbId, keepers, activeKeeperMaster);

		if (!dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId)) {
			List<RedisMeta> slaves = dcMetaCache.getShardRedises(clusterDbId, shardDbId);
			logger.info("[keeperActiveElected][current dc backup, set slave to new keeper]{},{},{}", clusterDbId, shardDbId,
					slaves);
			keeperStateChangeJob.setActiveSuccessCommand(new ConditionalCommand<>(
					new DefaultSlaveOfJob(slaves, activeKeeper.getIp(), activeKeeper.getPort(), clientPool, scheduled, executors),
					() -> !dcMetaCache.isCurrentDcPrimary(clusterDbId, shardDbId), true));
		}
		
		keyedOneThreadTaskExecutor.execute(new Pair<>(clusterDbId, shardDbId), keeperStateChangeJob);
	}

	@Override
	protected void doDispose() throws Exception {
		
		keyedOneThreadTaskExecutor.destroy();
		executors.shutdown();
		super.doDispose();
	}
	
	public void setcurrentMetaManager(CurrentMetaManager currentMetaManager) {
		this.currentMetaManager = currentMetaManager;
	}
	
	public void setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
	}
	
	public void setClientPool(SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool) {
		this.clientPool = clientPool;
	}
	
	public void setScheduled(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}

	public void setExecutors(ExecutorService executors) {
		this.executors = executors;
	}

	@VisibleForTesting
	protected void setConfig(MetaServerConfig config) {
		this.config = config;
	}
}
