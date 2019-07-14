package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
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
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 *         Sep 4, 2016
 */
@Component
public class DefaultKeeperManager extends AbstractCurrentMetaObserver implements KeeperManager, TopElement {

	@Autowired
	private MetaServerConfig config;

	private int deadKeeperCheckIntervalMilli = Integer
			.parseInt(System.getProperty("deadKeeperCheckIntervalMilli", "15000"));

	@Autowired
	private KeeperStateController keeperStateController;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	@Resource(name = MetaServerContextConfig.CLIENT_POOL)
	private SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;

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

		String clusterId = comparator.getCurrent().getId();
		comparator.accept(new ClusterComparatorVisitor(clusterId));
	}

	@Override
	protected void handleClusterDeleted(ClusterMeta clusterMeta) {

		for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
			for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
				removeKeeper(clusterMeta.getId(), shardMeta.getId(), keeperMeta);
			}
		}
	}

	private void removeKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		try {
			keeperStateController.removeKeeper(new KeeperTransMeta(clusterId, shardId, keeperMeta));
		} catch (Exception e) {
			logger.error(String.format("[removeKeeper]%s:%s,%s", clusterId, shardId, keeperMeta), e);
		}
	}

	private void addKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		try {
			keeperStateController.addKeeper(new KeeperTransMeta(clusterId, shardId, keeperMeta));
		} catch (Exception e) {
			logger.error(String.format("[addKeeper]%s:%s,%s", clusterId, shardId, keeperMeta), e);
		}
	}

	@Override
	protected void handleClusterAdd(ClusterMeta clusterMeta) {

		for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
			for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
				addKeeper(clusterMeta.getId(), shardMeta.getId(), keeperMeta);
			}
		}
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

		protected void doCheckShard(String clusterId, ShardMeta shardMeta) {
			String shardId = shardMeta.getId();
			List<KeeperMeta> allKeepers = shardMeta.getKeepers();
			List<KeeperMeta> aliveKeepers = currentMetaManager.getSurviveKeepers(clusterId, shardId);
			List<KeeperMeta> deadKeepers = getDeadKeepers(allKeepers, aliveKeepers);

			if (deadKeepers.size() > 0) {
				logger.info("[doCheck][dead keepers]{}", deadKeepers);
			}
			for (KeeperMeta deadKeeper : deadKeepers) {
				try {
					keeperStateController.addKeeper(new KeeperTransMeta(clusterId, shardId, deadKeeper));
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

		public ClusterComparatorVisitor(String clusterId) {
			this.clusterId = clusterId;
		}

		@Override
		public void visitAdded(ShardMeta added) {
			logger.info("[visitAdded][add shard]{}", added);
			for (KeeperMeta keeperMeta : added.getKeepers()) {
				addKeeper(clusterId, added.getId(), keeperMeta);
			}
		}

		@Override
		public void visitModified(@SuppressWarnings("rawtypes") MetaComparator comparator) {

			ShardMetaComparator shardMetaComparator = (ShardMetaComparator) comparator;
			shardMetaComparator.accept(new ShardComparatorVisitor(clusterId, shardMetaComparator.getCurrent().getId()));

		}

		@Override
		public void visitRemoved(ShardMeta removed) {
			logger.info("[visitRemoved][remove shard]{}", removed);
			for (KeeperMeta keeperMeta : removed.getKeepers()) {
				removeKeeper(clusterId, removed.getId(), keeperMeta);
			}
		}
	}

	protected class ShardComparatorVisitor implements MetaComparatorVisitor<Redis> {

		private String clusterId;
		private String shardId;

		protected ShardComparatorVisitor(String clusterId, String shardId) {
			this.clusterId = clusterId;
			this.shardId = shardId;
		}

		@Override
		public void visitAdded(Redis added) {

			if (added instanceof KeeperMeta) {
				addKeeper(clusterId, shardId, (KeeperMeta) added);
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
				removeKeeper(clusterId, shardId, (KeeperMeta) removed);
			} else {
				logger.debug("[visitAdded][do nothng]{}", removed);
			}
		}
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
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
					doCheckShard(clusterId, shardMeta);
				}
			}
		}

		protected abstract void doCheckShard(String clusterId, ShardMeta shardMeta);
	}

	private final String STATE = "state";
	private final String MASTER_HOST = "master_host";
	private final String MASTER_PORT = "master_port";

	public class KeeperStateAlignChecker extends AbstractKeeperStateChecker {

		@Override
		protected void doCheckShard(String clusterId, ShardMeta shardMeta) {
			String shardId = shardMeta.getId();
			List<KeeperMeta> keeperMetas = currentMetaManager.getSurviveKeepers(clusterId, shardId);
			for (KeeperMeta keeperMeta : keeperMetas) {
				InfoCommand infoCommand = new InfoCommand(
						clientPool.getKeyPool(new DefaultEndPoint(keeperMeta.getIp(), keeperMeta.getPort())),
						InfoCommand.INFO_TYPE.REPLICATION,
						scheduled
				);
				infoCommand.logRequest(false);
				infoCommand.logResponse(false);
				infoCommand.execute().addListener(new CommandFutureListener<String>() {
					@Override
					public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
						if (commandFuture.isSuccess()) {
							InfoResultExtractor extractor = new InfoResultExtractor(commandFuture.getNow());
							AbstractKeeperInfoChecker infoChecker = createKeeperInfoChecker(keeperMeta, keeperMetas,
									clusterId, shardId, extractor);
							infoChecker.check();
						}
					}
				});
			}
		}
	}

	private AbstractKeeperInfoChecker createKeeperInfoChecker(KeeperMeta keeper, List<KeeperMeta> keepers,
															  String clusterId, String shardId, InfoResultExtractor extractor) {
		if(keeper.isActive()) {
			return new ActiveKeeperInfoChecker(extractor, clusterId, shardId, keepers);
		} else {
			return new BackupKeeperInfoChecker(extractor, clusterId, shardId, keepers);
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
		protected List<KeeperMeta> keepers;

		public AbstractKeeperInfoChecker(InfoResultExtractor extractor, String clusterId, String shardId, List<KeeperMeta> keepers) {
			this.extractor = extractor;
			this.clusterId = clusterId;
			this.shardId = shardId;
			this.keepers = keepers;
		}

		protected void check() {
			if(!isKeeperStateOk() || !isMasterMatched()) {
				doCorrect();
			}
		}

		protected boolean isMasterMatched() {
			return getMasterHost().equals(extractor.extract(MASTER_HOST))
					&& getMasterPort() == Integer.parseInt(extractor.extract(MASTER_PORT));
		}

		protected void doCorrect() {
			KeeperStateChangeJob job = createKeeperStateChangeJob(clusterId, keepers,
					currentMetaManager.getKeeperMaster(clusterId, shardId));
			job.execute(executors).addListener(new CommandFutureListener<Void>() {
				@Override
				public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
					logger.info("[{}][KeeperInfoCorrect]cluster: {}, shard: {}, result: {}",
							getClass(), clusterId, shardId, commandFuture.isSuccess());
				}
			});
		}

		protected abstract boolean isKeeperStateOk();
		protected abstract String getMasterHost();
		protected abstract int getMasterPort();


	}

	public class ActiveKeeperInfoChecker extends AbstractKeeperInfoChecker {

		private Pair<String, Integer> master;

		public ActiveKeeperInfoChecker(InfoResultExtractor extractor, String clusterId, String shardId, List<KeeperMeta> keepers) {
			super(extractor, clusterId, shardId, keepers);
			master = currentMetaManager.getKeeperMaster(clusterId, shardId);
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

		public BackupKeeperInfoChecker(InfoResultExtractor extractor, String clusterId, String shardId, List<KeeperMeta> keepers) {
			super(extractor, clusterId, shardId, keepers);
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
}
