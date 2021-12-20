package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHANGE_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomeBackupAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.BecomePrimaryAction;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.ClusterShardCachedNewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.FirstNewMasterChooser;
import com.ctrip.xpipe.redis.meta.server.job.ChangePrimaryDcJob;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.ctrip.xpipe.redis.core.service.AbstractService.DEFAULT_SO_TIMEOUT;
import static com.ctrip.xpipe.redis.meta.server.dcchange.impl.AbstractChangePrimaryDcAction.DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS;
import static com.ctrip.xpipe.redis.meta.server.dcchange.impl.AbstractNewMasterChooser.CHECK_NEW_MASTER_TIMEOUT_SECONDS;
import static com.ctrip.xpipe.redis.meta.server.dcchange.impl.DefaultSentinelManager.DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI;
import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.*;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
@Component
public class DefaultChangePrimaryDcAction implements ChangePrimaryDcAction {
	
	private static Logger logger = LoggerFactory.getLogger(DefaultChangePrimaryDcAction.class);

	private static final int defaultTimeout = Integer.parseInt(System.getProperty("CHANGE_PRIMARY_DC_ACTION_TIMEOUT", "1000"));

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private XpipeNettyClientKeyedObjectPool keyedObjectPool;

	@Resource(name = REPLICATION_ADJUST_SCHEDULED)
	private ScheduledExecutorService scheduled;

	@Resource(name = REPLICATION_ADJUST_EXECUTOR)
	private ExecutorService executors;

	@Resource(name = AbstractSpringConfigContext.CLUSTER_SHARD_ADJUST_EXECUTOR)
	private KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long> >  clusterShardExecutors;

	@Autowired
	private DcMetaCache  dcMetaCache;
	
	@Autowired
	private CurrentMetaManager currentMetaManager;
	
	@Autowired
	private SentinelManager sentinelManager;
	
	@Autowired
	private MultiDcService multiDcService;

	@Autowired
	private OffsetWaiter offsetWaiter;

	@Autowired
	private CurrentClusterServer currentClusterServer;

	@Autowired
	private MetaServerConfig metaServerConfig;

	@Override
	public PrimaryDcChangeMessage changePrimaryDc(Long clusterDbId, Long shardDbId, String newPrimaryDc, MasterInfo masterInfo) {

		ExecutionLog executionLog = new ExecutionLog(String.format("meta server:%s", currentClusterServer.getClusterInfo()));

		if(!currentMetaManager.hasCluster(clusterDbId)){
			logger.info("[changePrimaryDc][not interested in this cluster]{}, {}", clusterDbId, shardDbId);
			executionLog.info("not interested in this cluster:" + clusterDbId);
			return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, executionLog.getLog());
		}
		
		ChangePrimaryDcAction changePrimaryDcAction = null;
		if(newPrimaryDc.equalsIgnoreCase(dcMetaCache.getCurrentDc())){
			logger.info("[doChangePrimaryDc][become primary]{}, {}, {}", clusterDbId, shardDbId, newPrimaryDc);
			changePrimaryDcAction = new BecomePrimaryAction(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, sentinelManager,
					offsetWaiter, executionLog, keyedObjectPool, createNewMasterChooser(clusterDbId, shardDbId), scheduled, executors);
			ChangePrimaryDcJob changePrimaryDcJob = createChangePrimaryDcJob(changePrimaryDcAction, clusterDbId, shardDbId,
					newPrimaryDc, masterInfo);
			int timeout = DEFAULT_SO_TIMEOUT / 2;
			try {
				clusterShardExecutors.clearAndExecute(new Pair<>(clusterDbId, shardDbId), changePrimaryDcJob);
				waitForCommandStart(changePrimaryDcJob);
				return changePrimaryDcJob.future().get(timeout, TimeUnit.MILLISECONDS);
			} catch (TimeoutException|InterruptedException e) {
				logger.error("[changePrimaryDc][execute may timeout][fall to run directly]{}, {}, {}", clusterDbId, shardDbId, newPrimaryDc, e);
				// In case task queue is blocked, we do downgrade(or a double-insurance)
				try {
					return createChangePrimaryDcJob(changePrimaryDcAction, clusterDbId, shardDbId, newPrimaryDc, masterInfo)
							.execute().get();
				} catch (Exception innerException) {
					logger.error("[changePrimaryDc][try direct-run failed]{}, {}, {}", clusterDbId, shardDbId, newPrimaryDc, e);
					return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, executionLog.getLog());
				}
			}catch (Exception e) {
				logger.error("[changePrimaryDc][execute by adjust executors fail]" + clusterDbId + "," + shardDbId + "," + newPrimaryDc, e);
				return new PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT.FAIL, executionLog.getLog());
			}
		} else {
			logger.info("[doChangePrimaryDc][become backup]{}, {}, {}", clusterDbId, shardDbId, newPrimaryDc);
			changePrimaryDcAction = new BecomeBackupAction(clusterDbId, shardDbId, dcMetaCache, currentMetaManager, sentinelManager, executionLog, keyedObjectPool, multiDcService, scheduled, executors);
			return changePrimaryDcAction.changePrimaryDc(clusterDbId, shardDbId, newPrimaryDc, masterInfo);
		}
	}

	private void waitForCommandStart(ChangePrimaryDcJob changePrimaryDcJob) throws TimeoutException, InterruptedException {
		int timeout = Math.max(DEFAULT_SO_TIMEOUT - DEFAULT_MIGRATION_SENTINEL_COMMAND_TIMEOUT_MILLI * 5
				- metaServerConfig.getWaitforOffsetMilli() - CHECK_NEW_MASTER_TIMEOUT_SECONDS * 2 * 1000
				- DEFAULT_CHANGE_PRIMARY_WAIT_TIMEOUT_SECONDS * 1000, defaultTimeout);
		long endTime = System.currentTimeMillis() + timeout;
		while(System.currentTimeMillis() < endTime) {
			if (changePrimaryDcJob.isStarted()) {
				return;
			}
			Thread.sleep(10);
		}
		throw new TimeoutException("ChangePrimaryDcJob has not started for " + timeout + "ms");
	}

	@VisibleForTesting
	protected ChangePrimaryDcJob createChangePrimaryDcJob(ChangePrimaryDcAction changePrimaryDcAction, Long clusterDbId,
														  Long shardDbId, String newPrimaryDc, MasterInfo masterInfo) {
		return new ChangePrimaryDcJob(changePrimaryDcAction, clusterDbId, shardDbId,
				newPrimaryDc, masterInfo);
	}

	private NewMasterChooser wrapCachedNewMasterChooser(Long clusterDbId, Long shardDbId, NewMasterChooser chooser) {
		return ClusterShardCachedNewMasterChooser.wrapChooser(clusterDbId, shardDbId, chooser, metaServerConfig::getNewMasterCacheTimeoutMilli, scheduled);
	}

	private NewMasterChooser createNewMasterChooser(Long clusterDbId, Long shardDbId) {
		return wrapCachedNewMasterChooser(clusterDbId, shardDbId, new FirstNewMasterChooser(keyedObjectPool, scheduled, executors));
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
		this.keyedObjectPool = keyedObjectPool;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setScheduled(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setExecutors(ExecutorService executors) {
		this.executors = executors;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setClusterShardExecutors(KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long>> clusterShardExecutors) {
		this.clusterShardExecutors = clusterShardExecutors;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setDcMetaCache(DcMetaCache dcMetaCache) {
		this.dcMetaCache = dcMetaCache;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setCurrentMetaManager(CurrentMetaManager currentMetaManager) {
		this.currentMetaManager = currentMetaManager;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setSentinelManager(SentinelManager sentinelManager) {
		this.sentinelManager = sentinelManager;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setMultiDcService(MultiDcService multiDcService) {
		this.multiDcService = multiDcService;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setOffsetWaiter(OffsetWaiter offsetWaiter) {
		this.offsetWaiter = offsetWaiter;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setCurrentClusterServer(CurrentClusterServer currentClusterServer) {
		this.currentClusterServer = currentClusterServer;
		return this;
	}

	@VisibleForTesting
	protected DefaultChangePrimaryDcAction setMetaServerConfig(MetaServerConfig metaServerConfig) {
		this.metaServerConfig = metaServerConfig;
		return this;
	}
}
