package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperElectorManager;
import com.ctrip.xpipe.redis.meta.server.keeper.impl.AbstractCurrentMetaObserver;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
@Component
public class DefaultKeeperElectorManager extends AbstractCurrentMetaObserver implements KeeperElectorManager, Observer, TopElement{

	public static final int  FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI = 50;

	@Autowired
	private ZkClient zkClient;

	@Autowired
	private KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager;

	private void observeLeader(final ClusterMeta cluster) {

		logger.info("[observeLeader]{}", cluster.getId());

		for (final ShardMeta shard : cluster.getShards().values()) {
			observerShardLeader(cluster.getId(), shard.getId(), cluster.getDbId(), shard.getDbId());
		}
	}

	protected void observerShardLeader(final String clusterId, final String shardId, final Long clusterDbId, final Long shardDbId) {
		logger.info("[observerShardLeader]{},{}({},{})", clusterId, shardId, clusterDbId, shardDbId);

		final CuratorFramework client = zkClient.get();
		if(currentMetaManager.watchIfNotWatched(clusterId, shardId)){
			try {
                List<PathChildrenCache> pathChildrenCaches = new ArrayList<>();
                pathChildrenCaches.add(buildPathChildrenCacheByName(clusterId, shardId, client));
				if (null != clusterDbId && null != shardDbId)
					pathChildrenCaches.add(buildPathChildrenCacheByDbId(clusterDbId, shardDbId, client));

				ReentrantLock lock = new ReentrantLock();
				pathChildrenCaches.forEach(pathChildrenCache -> {
					pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {

						private AtomicBoolean isFirst = new AtomicBoolean(true);
						@Override
						public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

							if(isFirst.get()){
								isFirst.set(false);
								try {
									logger.info("[childEvent][first sleep]{}", FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
									TimeUnit.MILLISECONDS.sleep(FIRST_PATH_CHILDREN_CACHE_SLEEP_MILLI);
								}catch (Exception e){
									logger.error("[childEvent]", e);
								}
							}

							try {
								lock.lock();
								logger.info("[childEvent]{}, {}, {}, {}", clusterId, shardId, event.getType(), ZkUtils.toString(event.getData()));
								updateShardLeader(aggregateChildData(pathChildrenCaches), clusterId, shardId);
							} finally {
								lock.unlock();
							}
						}
					});

					try {
						pathChildrenCache.start();
					} catch (Exception e) {
						logger.info("[observerShardLeader][{}-{}] start cache fail {}", clusterId, shardId, pathChildrenCache, e);
					}
				});

                registerJob(clusterId, shardId, new Releasable() {
					@Override
					public void release() throws Exception {
						pathChildrenCaches.forEach(pathChildrenCache -> {
							try{
								logger.info("[release][release children cache]{}, {}", clusterId, shardId);
								pathChildrenCache.close();
							}catch (Exception e){
								logger.error(String.format("[release][release children cache]%s, %s", clusterId, shardId), e);
							}
						});
					}
				});
			} catch (Exception e) {
				logger.error("[observerShardLeader]" + clusterId + " " + shardId, e);
			}
		}else{
			logger.warn("[observerShardLeader][already watched]{}, {}", clusterId, shardId);
		}
	}

	private PathChildrenCache buildPathChildrenCacheByName(String clusterId, String shardId, CuratorFramework client) {
		final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		logger.info("[observerShardLeader][add PathChildrenCache]{}, {}, {}", clusterId, shardId, leaderLatchPath);
		return new PathChildrenCache(client, leaderLatchPath, true,
				XpipeThreadFactory.create(String.format("PathChildrenCache:%s-%s", clusterId, shardId)));
	}

	private PathChildrenCache buildPathChildrenCacheByDbId(Long clusterDbId, Long shardDbId, CuratorFramework client) {
		final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterDbId, shardDbId);
		logger.info("[observerShardLeader][add PathChildrenCache]{}, {}, {}", clusterDbId, shardDbId, leaderLatchPath);
		return new PathChildrenCache(client, leaderLatchPath, true,
				XpipeThreadFactory.create(String.format("PathChildrenCache:cluster_%d-shard_%d", clusterDbId, shardDbId)));
	}

	private List<List<ChildData>> aggregateChildData(List<PathChildrenCache> pathChildrenCaches) {
		List<List<ChildData>> children = new ArrayList<>();
		pathChildrenCaches.forEach(pathChildrenCache -> children.add(pathChildrenCache.getCurrentData()));
		return children;
	}

	private LockInternalsSorter sorter = new LockInternalsSorter() {
		@Override
		public String fixForSorting(String str, String lockName) {
			return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
		}
	};

	protected List<ChildData> sortChildData(List<ChildData> childrenData) {
		List<String> childrenPaths = new LinkedList<>();
		childrenData.forEach(childData -> childrenPaths.add(childData.getPath()));
		List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, childrenPaths);

		List<ChildData> sortedChildrenData = new ArrayList<>(childrenData.size());
		for(String path : sortedChildren){
			for(ChildData childData : childrenData){
				if(path.equals(childData.getPath())){
					sortedChildrenData.add(childData);
					break;
				}
			}
		}

		return sortedChildrenData;
	}

	protected void updateShardLeader(List<List<ChildData>> childrenDataList, String clusterId, String shardId){
		logger.info("[updateShardLeader]{}, {}, {}", clusterId, shardId, childrenDataList);
		List<KeeperMeta> survivalKeepers = new ArrayList<>();
		int expectedKeepers = 0;

		for (List<ChildData> childrenData: childrenDataList) {
			expectedKeepers += childrenData.size();
			sortChildData(childrenData).forEach(childData -> {
				try {
					String data = new String(childData.getData());
					KeeperMeta keeper = JsonCodec.INSTANCE.decode(data, KeeperMeta.class);
					logger.info("[updateShardLeader] getkeeper {}", keeper);
					survivalKeepers.add(keeper);
				} catch (Throwable th) {
					logger.info("[updateShardLeader][{},{}] decode child data fail {}", clusterId, shardId, childData, th);
				}
			});
		}

		if(survivalKeepers.size() != expectedKeepers){
			throw new IllegalStateException(String.format("[children data not equal with survival keepers]%s, %s", childrenDataList, survivalKeepers));
		}

		KeeperActiveElectAlgorithm klea = keeperActiveElectAlgorithmManager.get(clusterId, shardId);
		KeeperMeta activeKeeper = klea.select(clusterId, shardId, survivalKeepers);
		currentMetaManager.setSurviveKeepers(clusterId, shardId, survivalKeepers, activeKeeper);
	}

	@Override
	protected void handleClusterModified(ClusterMetaComparator comparator) {

		String clusterId = comparator.getCurrent().getId();
		Long clusterDbId = comparator.getCurrent().getDbId();

		for(ShardMeta shardMeta : comparator.getAdded()){
			try {
				observerShardLeader(clusterId, shardMeta.getId(), clusterDbId, shardMeta.getDbId());
			} catch (Exception e) {
				logger.error("[handleClusterModified]" + clusterId + "," + shardMeta.getId(), e);
			}
		}
	}

	@Override
	protected void handleClusterAdd(ClusterMeta clusterMeta) {
		try {
			observeLeader(clusterMeta);
		} catch (Exception e) {
			logger.error("[handleClusterAdd]" + clusterMeta.getId(), e);
		}
	}

	@Override
	protected void handleClusterDeleted(ClusterMeta clusterMeta) {
		//nothing to do
	}

	@Override
	public Set<ClusterType> getSupportClusterTypes() {
		return Collections.singleton(ClusterType.ONE_WAY);
	}

	public void setKeeperActiveElectAlgorithmManager(
			KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager) {
		this.keeperActiveElectAlgorithmManager = keeperActiveElectAlgorithmManager;
	}
}
