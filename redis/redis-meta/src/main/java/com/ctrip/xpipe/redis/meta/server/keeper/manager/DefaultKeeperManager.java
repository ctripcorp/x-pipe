package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.Hints;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.concurrent.KeyedOneThreadMutexableTaskExecutor;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.KeeperIndexState;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.exception.KeeperStateInCorrectException;
import com.ctrip.xpipe.redis.meta.server.job.KeeperIndexChangeJob;
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
import java.util.*;
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
	private KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long>> clusterShardExecutor;

	private ScheduledFuture<?> deadCheckFuture;

	private ScheduledFuture<?> keeperInfoCheckFuture;

	private ScheduledFuture<?> keeperSetIndexFuture;

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

		keeperSetIndexFuture = scheduled.scheduleWithFixedDelay(new KeeperIndexChecker(), config.getKeeperSetIndexInterval(),
				config.getKeeperSetIndexInterval(), TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
		deadCheckFuture.cancel(true);
		keeperInfoCheckFuture.cancel(true);
		keeperSetIndexFuture.cancel(true);
		executors.shutdownNow();
	}

	@Override
	protected void handleClusterModified(ClusterMetaComparator comparator) {

		ClusterMeta cluster = comparator.getCurrent();
		comparator.accept(new ClusterComparatorVisitor(cluster.getType(), cluster.getDbId()));
	}

	@Override
	protected void handleClusterDeleted(ClusterMeta clusterMeta) {

		Long clusterDbId = clusterMeta.getDbId();
		for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
			for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
				removeKeeper(clusterDbId, shardMeta.getDbId(), keeperMeta);
			}
		}
	}

	private void removeKeeper(Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		try {
			keeperStateController.removeKeeper(new KeeperTransMeta(clusterDbId, shardDbId, keeperMeta));
		} catch (Exception e) {
			logger.error(String.format("[removeKeeper]cluster_%s:shard_%s,%s", clusterDbId, shardDbId, keeperMeta), e);
		}
	}

	private void addKeeper(Long clusterDbId, Long shardDbId, KeeperMeta keeperMeta) {
		try {
			KeeperTransMeta keeperTransMeta = new KeeperTransMeta(clusterDbId, shardDbId, keeperMeta);
			keeperStateController.addKeeper(keeperTransMeta);
		} catch (Exception e) {
			logger.error(String.format("[addKeeper]cluster_%s:shard_%s,%s", clusterDbId, shardDbId, keeperMeta), e);
		}
	}

	@Override
	protected void handleClusterAdd(ClusterMeta clusterMeta) {

		for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
			for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
				addKeeper(clusterMeta.getDbId(), shardMeta.getDbId(), keeperMeta);
			}
		}
	}

	@Override
	public Set<ClusterType> getSupportClusterTypes() {
		return Collections.singleton(ClusterType.ONE_WAY);
	}

	protected List<KeeperMeta> getKeepersSubtraction(List<KeeperMeta> allKeepers, List<KeeperMeta> subtractKeepers) {

		List<KeeperMeta> result = new LinkedList<>();
		for (KeeperMeta allOne : allKeepers) {
			boolean exist = false;
			for (KeeperMeta subtractKeeper : subtractKeepers) {
				if (ObjectUtils.equals(subtractKeeper.getIp(), allOne.getIp())
						&& ObjectUtils.equals(subtractKeeper.getPort(), allOne.getPort())) {
					exist = true;
					break;
				}
			}
			if (!exist) {
				result.add(allOne);
			}
		}
		return result;
	}

	public class DeadKeeperChecker extends AbstractKeeperStateChecker {

		protected void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {
			Long clusterDbId = clusterMeta.getDbId();
			Long shardDbId = shardMeta.getDbId();
			List<KeeperMeta> metaKeepers = shardMeta.getKeepers();
			List<KeeperMeta> aliveKeepers = currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId);
			List<KeeperMeta> deadKeepers = getKeepersSubtraction(metaKeepers, aliveKeepers);
			List<KeeperMeta> removedKeepers = getKeepersSubtraction(aliveKeepers, metaKeepers);

			if (deadKeepers.size() > 0) {
				logger.info("[doCheck][dead keepers]{}", deadKeepers);
				addDeadKeepers(deadKeepers, clusterDbId, shardDbId);
			}

			if (deadKeepers.size() == 0 && removedKeepers.size() > 0) {
				logger.info("[doCheck][removed keepers]{}", removedKeepers);
				removeRemovedKeepers(removedKeepers, clusterDbId, shardDbId);
			}

		}

	}

	private void addDeadKeepers(List<KeeperMeta> deadKeepers, Long clusterDbId, Long shardDbId) {
		for (KeeperMeta deadKeeper : deadKeepers) {
			try {
				KeeperTransMeta keeperTransMeta = new KeeperTransMeta(clusterDbId, shardDbId, deadKeeper);
				keeperStateController.addKeeper(keeperTransMeta);
			} catch (ResourceAccessException e) {
				logger.error(String.format("[check dead keepers]cluster_%d,shard_%d, keeper:%s, error:%s", clusterDbId, shardDbId,
						deadKeeper, e.getMessage()));
			} catch (Throwable th) {
				logger.error("[doCheck][dead keepers]", th);
			}
		}
	}

	private void removeRemovedKeepers(List<KeeperMeta> removedKeepers, Long clusterDbId, Long shardDbId) {
		for (KeeperMeta removedKeeper : removedKeepers) {
			try {
				KeeperTransMeta keeperTransMeta = new KeeperTransMeta(clusterDbId, shardDbId, removedKeeper);
				keeperStateController.removeKeeper(keeperTransMeta);
			} catch (ResourceAccessException e) {
				logger.error(String.format("[check removed keepers]cluster_%d,shard_%d, keeper:%s, error:%s", clusterDbId, shardDbId,
						removedKeeper, e.getMessage()));
			} catch (Throwable th) {
				logger.error("[doCheck][removed keepers]", th);
			}
		}
	}

	protected class ClusterComparatorVisitor implements MetaComparatorVisitor<ShardMeta> {

		private Long clusterDbId;
		private String clusterType;

		public ClusterComparatorVisitor(String clusterType, Long clusterDbId) {
			this.clusterType = clusterType;
			this.clusterDbId = clusterDbId;
		}

		@Override
		public void visitAdded(ShardMeta added) {
			logger.info("[visitAdded][add shard]{}", added);
			for (KeeperMeta keeperMeta : added.getKeepers()) {
				addKeeper(clusterDbId, added.getDbId(), keeperMeta);
			}
		}

		@Override
		public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {

			ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator;
			ShardMeta shard = shardMetaComparator.getCurrent();
			shardMetaComparator.accept(new ShardComparatorVisitor(clusterType, clusterDbId, shard.getDbId()));

		}

		@Override
		public void visitRemoved(ShardMeta removed) {
			logger.info("[visitRemoved][remove shard]{}", removed);
			for (KeeperMeta keeperMeta : removed.getKeepers()) {
				removeKeeper(clusterDbId, removed.getDbId(), keeperMeta);
			}
		}
	}

	protected class ShardComparatorVisitor implements MetaComparatorVisitor<InstanceNode> {

		private String clusterType;
		private Long clusterDbId;
		private Long shardDbId;

		protected ShardComparatorVisitor(String clusterType, Long clusterDbId, Long shardDbId) {
			this.clusterType = clusterType;
			this.clusterDbId = clusterDbId;
			this.shardDbId = shardDbId;
		}

		@Override
		public void visitAdded(InstanceNode added) {

			if (added instanceof KeeperMeta) {
				addKeeper(clusterDbId, shardDbId, (KeeperMeta) added);
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
		public void visitRemoved(InstanceNode removed) {

			if (removed instanceof KeeperMeta) {
				removeKeeper(clusterDbId, shardDbId, (KeeperMeta) removed);
			} else {
				logger.debug("[visitAdded][do nothng]{}", removed);
			}
		}
	}

	/**
	 * for primary dc, keeper's master should be a redis
	 * for backup dc, keeper's master should not be a redis
	 * because the current meta may delay, we need to make sure the relationship is correct*/
	protected boolean isCurrentMetaKeeperMasterMatch(Long clusterDbId, Long shardDbId) {

		Pair<String, Integer> master = currentMetaManager.getKeeperMaster(clusterDbId, shardDbId);
		if (master == null) {
			return false;
		}

		if(metaCache.isCurrentDcPrimary(clusterDbId)) {
			return isKeeperMasterRedis(clusterDbId, shardDbId, master);
		} else {
			return !isKeeperMasterRedis(clusterDbId, shardDbId, master);
		}
	}

	private boolean isKeeperMasterRedis(Long clusterDbId, Long shardDbId, Pair<String, Integer> master) {
		List<RedisMeta> redises = metaCache.getShardRedises(clusterDbId, shardDbId);
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
			for (Long clusterDbId : currentMetaManager.allClusters()) {
				ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterDbId);
				if (!supportCluster(clusterMeta)) continue;
				for (ShardMeta shardMeta : clusterMeta.getAllShards().values()) {
					doCheckShard(clusterMeta, shardMeta);
				}
			}
		}

		protected abstract void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta);
	}

	public class KeeperIndexChecker extends AbstractKeeperStateChecker {
		@Override
		protected void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {

			long clusterDbId = clusterMeta.getDbId();
			long shardDbId = shardMeta.getDbId();

			if (metaCache.getClusterMeta(clusterDbId) == null) {
				return;
			}
			String hints = metaCache.getClusterMeta(clusterDbId).getHints();
			if (!Hints.parse(hints).contains(Hints.APPLIER_IN_CLUSTER)) {
				return;
			}

			List<KeeperMeta> survivedKeepers = currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId);

			KeeperIndexChangeJob job = createKeeperIndexChangeJob(survivedKeepers);

			job.future().addListener(new CommandFutureListener<Void>() {
				@Override
				public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
					logger.info("[{}][KeeperSetIndex]cluster_{}, shard_{}, result: {}",
							getClass(), clusterDbId, shardDbId, commandFuture.isSuccess());
				}
			});
			clusterShardExecutor.execute(Pair.from(clusterDbId, shardDbId), job);
		}
	}

	private final String STATE = "state";
	private final String MASTER_HOST = "master_host";
	private final String MASTER_PORT = "master_port";

	public class KeeperStateAlignChecker extends AbstractKeeperStateChecker {

		@Override
		protected void doCheckShard(ClusterMeta clusterMeta, ShardMeta shardMeta) {
			Long clusterDbId = clusterMeta.getDbId();
			Long shardDbId = shardMeta.getDbId();
			List<KeeperMeta> keeperMetas = currentMetaManager.getSurviveKeepers(clusterDbId, shardDbId);
			if (keeperMetas.isEmpty()) return;
			
			SequenceCommandChain sequenceCommandChain =
					new SequenceCommandChain(String.format("%s-cluster_%d-shard_%d", this.getClass().getSimpleName(), clusterDbId, shardDbId));
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
				causalChain.add(new KeeperInfoCheckCommand(keeperMeta, clusterDbId, shardDbId));
				sequenceCommandChain.add(causalChain);
			}
			sequenceCommandChain.execute(executors).addListener(new CommandFutureListener<Object>() {
				@Override
				public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
					if(!commandFuture.isSuccess()) {
						doCorrect(clusterDbId, shardDbId, keeperMetas);
					}
				}
			});
		}

		protected void doCorrect(Long clusterDbId, Long shardDbId, List<KeeperMeta> survivedKeepers) {
			//double check again
			if(!isCurrentMetaKeeperMasterMatch(clusterDbId, shardDbId)) {
				return;
			}
			KeeperStateChangeJob job = createKeeperStateChangeJob(clusterDbId, shardDbId, survivedKeepers,
					currentMetaManager.getKeeperMaster(clusterDbId, shardDbId));
			job.future().addListener(new CommandFutureListener<Void>() {
				@Override
				public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
					logger.info("[{}][KeeperInfoCorrect]cluster_{}, shard_{}, result: {}",
							getClass(), clusterDbId, shardDbId, commandFuture.isSuccess());
				}
			});
			clusterShardExecutor.execute(Pair.from(clusterDbId, shardDbId), job);
		}
	}

	protected class KeeperInfoCheckCommand extends CausalCommand<String, Boolean> {

		private KeeperMeta keeperMeta;

		private Long clusterDbId, shardDbId;

		private AbstractKeeperInfoChecker infoChecker;

		public KeeperInfoCheckCommand(KeeperMeta keeperMeta, Long clusterDbId, Long shardDbId) {
			this.keeperMeta = keeperMeta;
			this.clusterDbId = clusterDbId;
			this.shardDbId = shardDbId;
		}

		@Override
		protected void onSuccess(String info) {
			InfoResultExtractor extractor = new InfoResultExtractor(info);
			infoChecker = createKeeperInfoChecker(keeperMeta, clusterDbId, shardDbId, extractor);
			if(infoChecker.isValid() || infoChecker.shouldStop()) {
				future().setSuccess();
			} else {
				future().setFailure(new KeeperStateInCorrectException("Keeper Role not correct"));
			}

		}

		@Override
		protected void onFailure(Throwable throwable) {
			if (ExceptionUtils.getRootCause(throwable) instanceof CommandTimeoutException) {
				logger.debug("[onFailure][cluster_{}][shard_{}] ignore failure for command timeout", clusterDbId, shardDbId);
				future().setSuccess();
			} else {
				future().setFailure(throwable);
			}
		}
	}



	private AbstractKeeperInfoChecker createKeeperInfoChecker(KeeperMeta keeper, Long clusterDbId, Long shardDbId,
															  InfoResultExtractor extractor) {
		if(keeper.isActive()) {
			return new ActiveKeeperInfoChecker(extractor, clusterDbId, shardDbId);
		} else {
			return new BackupKeeperInfoChecker(extractor, clusterDbId, shardDbId);
		}
	}

	private KeeperIndexChangeJob createKeeperIndexChangeJob(List<KeeperMeta> keepers) {
        KeeperIndexState indexState = KeeperIndexState.ON;
		return new KeeperIndexChangeJob(keepers, indexState, clientPool, scheduled, executors);
	}

	private KeeperStateChangeJob createKeeperStateChangeJob(Long clusterDbId, Long shardDbId,
															List<KeeperMeta> keepers,
															Pair<String, Integer> master) {

		String dstDcId;
		if (metaCache.isCurrentShardParentCluster(clusterDbId, shardDbId)) {
			dstDcId = currentMetaManager.getClusterMeta(clusterDbId).getActiveDc();
		} else {
			dstDcId = metaCache.getUpstreamDc(metaCache.getCurrentDc(), clusterDbId, shardDbId);
		}
		RouteMeta routeMeta = currentMetaManager.getClusterRouteByDcId(dstDcId, clusterDbId);

		return new KeeperStateChangeJob(keepers, master, routeMeta, clientPool, scheduled, executors);
	}

	protected abstract class AbstractKeeperInfoChecker {
		protected InfoResultExtractor extractor;
		protected Long clusterDbId, shardDbId;

		public AbstractKeeperInfoChecker(InfoResultExtractor extractor, Long clusterDbId, Long shardDbId) {
			this.extractor = extractor;
			this.clusterDbId = clusterDbId;
			this.shardDbId = shardDbId;
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

		public ActiveKeeperInfoChecker(InfoResultExtractor extractor, Long clusterDbId, Long shardDbId) {
			super(extractor, clusterDbId, shardDbId);
			master = currentMetaManager.getKeeperMaster(clusterDbId, shardDbId);
		}

		@Override
		protected boolean shouldStop() {
			return !isCurrentMetaKeeperMasterMatch(clusterDbId, shardDbId);
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

		public BackupKeeperInfoChecker(InfoResultExtractor extractor, Long clusterDbId, Long shardDbId) {
			super(extractor, clusterDbId, shardDbId);
			activeKeeper = currentMetaManager.getKeeperActive(clusterDbId, shardDbId);
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
	protected void setClusterShardExecutor(KeyedOneThreadMutexableTaskExecutor<Pair<Long, Long>> clusterShardExecutor) {
		this.clusterShardExecutor = clusterShardExecutor;
	}

	@VisibleForTesting
	protected void setScheduled(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}

	@VisibleForTesting
	protected void setKeeperStateController(KeeperStateController keeperStateController) {
		this.keeperStateController = keeperStateController;
	}
}
