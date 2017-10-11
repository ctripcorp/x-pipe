package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.ShardMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
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

	private int deadKeeperCheckIntervalMilli = Integer
			.parseInt(System.getProperty("deadKeeperCheckIntervalMilli", "15000"));

	@Autowired
	private KeeperStateController keeperStateController;

	@Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
	private ScheduledExecutorService scheduled;

	private ScheduledFuture<?> deadCheckFuture;

	@Override
	protected void doStart() throws Exception {
		super.doStart();

		deadCheckFuture = scheduled.scheduleWithFixedDelay(new DeadKeeperChecker(), deadKeeperCheckIntervalMilli,
				deadKeeperCheckIntervalMilli, TimeUnit.MILLISECONDS);

	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
		deadCheckFuture.cancel(true);

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

	public class DeadKeeperChecker implements Runnable {

		@Override
		public void run() {

			try {
				doCheck();
			} catch (Throwable th) {
				logger.error("[run]", th);
			}

		}

		private void doCheck() {

			for (String clusterId : currentMetaManager.allClusters()) {
				ClusterMeta clusterMeta = currentMetaManager.getClusterMeta(clusterId);
				for (ShardMeta shardMeta : clusterMeta.getShards().values()) {

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
}
