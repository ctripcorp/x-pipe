package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.exception.KeeperStateInCorrectException;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author wenchao.meng
 *
 *         Sep 4, 2016
 */
@Component
public class DefaultKeeperManager extends AbstractCurrentMetaObserver implements KeeperManager, TopElement {

	private int deadKeeperCheckIntervalMilli = Integer
			.parseInt(System.getProperty("deadKeeperCheckIntervalMilli", "15000"));

	@Autowired
	private MetaServerConfig config;

	@Autowired
	private KeeperStateController keeperStateController;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;

	@Autowired
	private DcMetaCache metaCache;

	@Resource(name = AbstractSpringConfigContext.CLUSTER_SHARD_ADJUST_EXECUTOR)
	private KeyedOneThreadMutexableTaskExecutor<Pair<String, String>> clusterShardExecutor;

	private ScheduledFuture<?> deadCheckFuture;

	private ScheduledFuture<?> keeperInfoCheckFuture;

	private ExecutorService executors;

	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		executors = DefaultExecutorFactory
				.createAllowCoreTimeout("DefaultKeeperManager", Math.min(4, OsUtils.defaultMaxCoreThreadCount()))
				.createExecutorService();
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();

		deadCheckFuture = scheduled.scheduleWithFixedDelay(new DeadKeeperChecker(), deadKeeperCheckIntervalMilli,
				deadKeeperCheckIntervalMilli, TimeUnit.MILLISECONDS);

		keeperInfoCheckFuture = scheduled.scheduleWithFixedDelay(new KeeperStateAlignChecker(), config.getKeeperInfoCheckInterval(),
				config.getKeeperInfoCheckInterval(), TimeUnit.MILLISECONDS);

	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
		deadCheckFuture.cancel(true);
		keeperInfoCheckFuture.cancel(true);
		executors.shutdownNow();
	}

	@Override
	protected void handleClusterModified(ClusterMetaComparator comparator) {

		ClusterMeta cluster = comparator.getCurrent();
		comparator.accept(new ClusterComparatorVisitor(cluster.getId(), cluster.getDbId()));
	}

	@Override
	protected void handleClusterDeleted(ClusterMeta clusterMeta) {

		Long clusterDbId = clusterMeta.getDbId();
		for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
			for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
				removeKeeper(clusterMeta.getId(), shardMeta.getId(), clusterDbId, shardMeta.getDbId(), keeperMeta);
			}
		}
	}

	private void removeKeeper(String clusterId, String shardId, Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		try {
			keeperStateController.removeKeeper(new KeeperTransMeta(clusterId, shardId, clusterDbId, shardDbId, keeperMeta));
		} catch (Exception e) {
			logger.error(String.format("[removeKeeper]%s:%s,%s", clusterId, shardId, keeperMeta), e);
		}
	}

	private void addKeeper(String clusterId, String shardId, Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		try {
			keeperStateController.addKeeper(new KeeperTransMeta(clusterId, shardId, clusterDbId, shardDbId, keeperMeta));
		} catch (Exception e) {
			logger.error(String.format("[addKeeper]%s:%s,%s", clusterId, shardId, keeperMeta), e);
		}
	}

	@Override
	protected void handleClusterAdd(ClusterMeta clusterMeta) {

		for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
			for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
				addKeeper(clusterMeta.getId(), shardMeta.getId(), clusterMeta.getDbId(), shardMeta.getDbId(), keeperMeta);
			}
		}
	}

	@Override
	public Set<ClusterType> getSupportClusterTypes() {
		return Collections.singleton(ClusterType.ONE_WAY);
	}

	protected List<KeeperMeta> getDeadKeepers(List<KeeperMeta> allKeepers, List<KeeperMeta> aliveKeepers) {

		List<KeeperMeta> result = new LinkedList<>();
		for (KeeperMeta allOne : allKeepers) {
			boolean alive = false;
			for (KeeperMeta aliveOne : aliveKeepers) {
				if (ObjectUtils.equals(aliveOne.getIp(), allOne.getIp())
						&& ObjectUtils.equals(aliveOne.getPort(), allOne.getPort())) {
					alive = true;
					break;
				}
			}
			if (!alive) {
				result.add(allOne);
			}
		}
		return result;
	}

	public class DeadKeeperChecker extends AbstractKeeperStateChecker {

		protected void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {
			String clusterId = clusterMeta.getId();
			String shardId = shardMeta.getId();
			List<KeeperMeta> allKeepers = shardMeta.getKeepers();
			List<KeeperMeta> aliveKeepers = currentMetaManager.getSurviveKeepers(clusterId, shardId);
			List<KeeperMeta> deadKeepers = getDeadKeepers(allKeepers, aliveKeepers);

			if (deadKeepers.size() > 0) {
				logger.info("[doCheck][dead keepers]{}", deadKeepers);
			}
			for (KeeperMeta deadKeeper : deadKeepers) {
				try {
					keeperStateController.addKeeper(new KeeperTransMeta(clusterId, shardId, clusterMeta.getDbId(), shardMeta.getDbId(), deadKeeper));
				} catch (ResourceAccessException e) {
					logger.error(String.format("cluster:%s,shard:%s, keeper:%s, error:%s", clusterId, shardId,
							deadKeeper, e.getMessage()));
				} catch (Throwable th) {
					logger.error("[doCheck]", th);
				}
			}
		}

	}

	protected class ClusterComparatorVisitor implements MetaComparatorVisitor<ShardMeta> {

		private String clusterId;

		private Long clusterDbId;

		public ClusterComparatorVisitor(String clusterId, Long clusterDbId) {
			this.clusterId = clusterId;
			this.clusterDbId = clusterDbId;
		}

		@Override
		public void visitAdded(ShardMeta added) {
			logger.info("[visitAdded][add shard]{}", added);
			for (KeeperMeta keeperMeta : added.getKeepers()) {
				addKeeper(clusterId, added.getId(), clusterDbId, added.getDbId(), keeperMeta);
			}
		}

		@Override
		public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {

			ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator;
			ShardMeta shard = shardMetaComparator.getCurrent();
			shardMetaComparator.accept(new ShardComparatorVisitor(clusterId, shard.getId(), clusterDbId, shard.getDbId()));

		}

		@Override
		public void visitRemoved(ShardMeta removed) {
			logger.info("[visitRemoved][remove shard]{}", removed);
			for (KeeperMeta keeperMeta : removed.getKeepers()) {
				removeKeeper(clusterId, removed.getId(), clusterDbId, removed.getDbId(), keeperMeta);
			}
		}
	}

	protected class ShardComparatorVisitor implements MetaComparatorVisitor<Redis> {

		private String clusterId;
		private String shardId;
		private Long clusterDbId;
		private Long shardDbId;

		protected ShardComparatorVisitor(String clusterId, String shardId, Long clusterDbId, Long shardDbId) {
			this.clusterId = clusterId;
			this.shardId = shardId;
			this.clusterDbId = clusterDbId;
			this.shardDbId = shardDbId;
		}

		@Override
		public void visitAdded(Redis added) {

			if (added instanceof KeeperMeta) {
				addKeeper(clusterId, shardId, clusterDbId, shardDbId, (KeeperMeta) added);
			} else {
				logger.debug("[visitAdded][do nothng]{}", added);
			}
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void visitModified(MetaComparator comparator) {
			// nothing to do
		}

		@Override
		public void visitRemoved(Redis removed) {

			if (removed instanceof KeeperMeta) {
				removeKeeper(clusterId, shardId, clusterDbId, shardDbId, (KeeperMeta) removed);
			} else {
				logger.debug("[visitAdded][do nothng]{}", removed);
			}
		}
	}

	/**
	 * for primary dc, keeper's master should be a redis
	 * for backup dc, keeper's master should not be a redis
	 * because the current meta may delay, we need to make sure the relationship is correct*/
	protected boolean isCurrentMetaKeeperMasterMatch(String clusterId, String shardId) {
		if(metaCache.isCurrentDcPrimary(clusterId)) {
			return isKeeperMasterRedis(clusterId, shardId);
		} else {
			return !isKeeperMasterRedis(clusterId, shardId);
		}
	}

	private boolean isKeeperMasterRedis(String clusterId, String shardId) {
		List<RedisMeta> redises = metaCache.getShardRedises(clusterId, shardId);
		Pair<String, Integer> master = currentMetaManager.getKeeperMaster(clusterId, shardId);
		for (RedisMeta redis : redises) {
			if (redis.getPort().equals(master.getValue()) && redis.getIp().equals(master.getKey())) {
				return true;
			}
		}
		return false;
	}

	interface KeeperStateChecker extends Runnable {}

	private abstract class AbstractKeeperStateChecker implements KeeperStateChecker {
		@Override
		public void run() {

			try {
				doCheck();
			} catch (Throwable th) {
				logger.error("[run]", th);
			}

		}

		protected void doCheck() {
			for (String clusterId : currentMetaManager.allClusters()) {
				ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterId);
				if (!supportCluster(clusterMeta)) continue;
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					doCheckShard(clusterMeta, shardMeta);
				}
			}
		}

		protected abstract void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta);
	}

	private final String STATE = "state";
	private final String MASTER_HOST = "master_host";
	private final String MASTER_PORT = "master_port";

	public class KeeperStateAlignChecker extends AbstractKeeperStateChecker {

		@Override
		protected void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {
			String clusterId = clusterMeta.getId();
			String shardId = shardMeta.getId();
			List<KeeperMeta> keeperMetas = currentMetaManager.getSurviveKeepers(clusterId, shardId);
			if (keeperMetas.isEmpty()) return;
			
			SequenceCommandChain sequenceCommandChain =
					new SequenceCommandChain(String.format("%s-%s-%s", this.getClass().getSimpleName(), clusterId, shardId));
			for (KeeperMeta keeperMeta : keeperMetas) {
				InfoCommand infoCommand = new InfoCommand(
						clientPool.getKeyPool(new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort())),
						InfoCommand.INFO_TYPE.REPLICATION,
						scheduled
				);
				infoCommand.logRequest(false);
				infoCommand.logResponse(false);
				CausalChain causalChain = new CausalChain();
				causalChain.add(infoCommand);
				causalChain.add(new KeeperInfoCheckCommand(keeperMeta, clusterId, shardId));
				sequenceCommandChain.add(causalChain);
			}
			sequenceCommandChain.execute(executors).addListener(new CommandFutureListener<Object>() {
				@Override
				public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
					if(!commandFuture.isSuccess()) {
						doCorrect(clusterId, shardId, keeperMetas);
					}
				}
			});
		}

		protected void doCorrect(String clusterId, String shardId, List<KeeperMeta> survivedKeepers) {
			//double check again
			if(!isCurrentMetaKeeperMasterMatch(clusterId, shardId)) {
				return;
			}
			KeeperStateChangeJob job = createKeeperStateChangeJob(clusterId, survivedKeepers,
					currentMetaManager.getKeeperMaster(clusterId, shardId));
			job.future().addListener(new CommandFutureListener<Void>() {
				@Override
				public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
					logger.info("[{}][KeeperInfoCorrect]cluster: {}, shard: {}, result: {}",
							getClass(), clusterId, shardId, commandFuture.isSuccess());
				}
			});
			clusterShardExecutor.execute(Pair.from(clusterId, shardId), job);
		}
	}

	protected class KeeperInfoCheckCommand extends CausalCommand<String, Boolean> {

		private KeeperMeta keeperMeta;

		private String clusterId, shardId;

		private AbstractKeeperInfoChecker infoChecker;

		public KeeperInfoCheckCommand(KeeperMeta keeperMeta, String clusterId, String shardId) {
			this.keeperMeta = keeperMeta;
			this.clusterId = clusterId;
			this.shardId = shardId;
		}

		@Override
		protected void onSuccess(String info) {
			InfoResultExtractor extractor = new InfoResultExtractor(info);
			infoChecker = createKeeperInfoChecker(keeperMeta, clusterId, shardId, extractor);
			if(infoChecker.isValid() || infoChecker.shouldStop()) {
				future().setSuccess();
			} else {
				future().setFailure(new KeeperStateInCorrectException("Keeper Role not correct"));
			}

		}

		@Override
		protected void onFailure(Throwable throwable) {
			if (ExceptionUtils.getRootCause(throwable) instanceof CommandTimeoutException) {
				logger.debug("[onFailure][{}-{}] ignore failure for command timeout", clusterId, shardId);
				future().setSuccess();
			} else {
				future().setFailure(throwable);
			}
		}
	}



	private AbstractKeeperInfoChecker createKeeperInfoChecker(KeeperMeta keeper, String clusterId, String shardId,
															  InfoResultExtractor extractor) {
		if(keeper.isActive()) {
			return new ActiveKeeperInfoChecker(extractor, clusterId, shardId);
		} else {
			return new BackupKeeperInfoChecker(extractor, clusterId, shardId);
		}
	}

	private KeeperStateChangeJob createKeeperStateChangeJob(String clusterId, List<KeeperMeta> keepers,
															Pair<String, Integer> master) {

		RouteMeta routeMeta = currentMetaManager.randomRoute(clusterId);
		return new KeeperStateChangeJob(keepers, master, routeMeta, clientPool, scheduled, executors);
	}

	protected abstract class AbstractKeeperInfoChecker {
		protected InfoResultExtractor extractor;
		protected String clusterId, shardId;

		public AbstractKeeperInfoChecker(InfoResultExtractor extractor, String clusterId, String shardId) {
			this.extractor = extractor;
			this.clusterId = clusterId;
			this.shardId = shardId;
		}

		protected boolean shouldStop() {
			return false;
		}

		protected boolean isValid() {
			return isKeeperStateOk() && isMasterMatched();
		}

		protected boolean isMasterMatched() {
			return getMasterHost().equals(extractor.extract(MASTER_HOST))
					&& getMasterPort() == Integer.parseInt(extractor.extract(MASTER_PORT));
		}

		protected abstract boolean isKeeperStateOk();
		protected abstract String getMasterHost();
		protected abstract int getMasterPort();


	}

	public class ActiveKeeperInfoChecker extends AbstractKeeperInfoChecker {

		private Pair<String, Integer> master;

		public ActiveKeeperInfoChecker(InfoResultExtractor extractor, String clusterId, String shardId) {
			super(extractor, clusterId, shardId);
			master = currentMetaManager.getKeeperMaster(clusterId, shardId);
		}

		@Override
		protected boolean shouldStop() {
			return !isCurrentMetaKeeperMasterMatch(clusterId, shardId);
		}

		@Override
		protected boolean isKeeperStateOk() {
			return KeeperState.ACTIVE.name().equals(extractor.extract(STATE));
		}

		@Override
		protected String getMasterHost() {
			return master.getKey();
		}

		@Override
		protected int getMasterPort() {
			return master.getValue();
		}


	}

	public class BackupKeeperInfoChecker extends AbstractKeeperInfoChecker {

		private KeeperMeta activeKeeper;

		public BackupKeeperInfoChecker(InfoResultExtractor extractor, String clusterId, String shardId) {
			super(extractor, clusterId, shardId);
			activeKeeper = currentMetaManager.getKeeperActive(clusterId, shardId);
		}

		@Override
		protected boolean isKeeperStateOk() {
			return KeeperState.BACKUP.name().equals(extractor.extract(STATE));
		}

		@Override
		protected String getMasterHost() {
			return activeKeeper.getIp();
		}

		@Override
		protected int getMasterPort() {
			return activeKeeper.getPort();
		}


	}

	@VisibleForTesting
	protected DefaultKeeperManager setClientPool(SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool) {
		this.clientPool = clientPool;
		return this;
	}

	@VisibleForTesting
	protected DefaultKeeperManager setExecutors(ExecutorService executors) {
		this.executors = executors;
		return this;
	}

	@VisibleForTesting
	protected void setMetaCache(DcMetaCache metaCache) {
		this.metaCache = metaCache;
	}

	@VisibleForTesting
	protected void setClusterShardExecutor(KeyedOneThreadMutexableTaskExecutor<Pair<String, String>> clusterShardExecutor) {
		this.clusterShardExecutor = clusterShardExecutor;
	}

	@VisibleForTesting
	protected void setScheduled(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}
}
