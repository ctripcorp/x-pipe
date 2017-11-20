package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observer;
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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
			observerShardLeader(cluster.getId(), shard.getId());
		}
	}

	protected void observerShardLeader(final String clusterId, final String shardId) {

		logger.info("[observerShardLeader]{}, {}", clusterId, shardId);

		final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		final CuratorFramework client = zkClient.get();

		if(currentMetaManager.watchIfNotWatched(clusterId, shardId)){
			try {
                logger.info("[observerShardLeader][add PathChildrenCache]{}, {}", clusterId, shardId);
                PathChildrenCache pathChildrenCache = new PathChildrenCache(client, leaderLatchPath, true, XpipeThreadFactory.create(String.format("PathChildrenCache:%s-%s", clusterId, shardId)));
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

                    	logger.info("[childEvent]{}, {}, {}, {}", clusterId, shardId, event.getType(), ZkUtils.toString(event.getData()));
                        updateShardLeader(leaderLatchPath, pathChildrenCache.getCurrentData(), clusterId, shardId);
                    }
                });
				currentMetaManager.addResource(clusterId, shardId, new Releasable() {
                    @Override
                    public void release() throws Exception {

                    	try{
							logger.info("[release][release children cache]{}, {}", clusterId, shardId);
							pathChildrenCache.close();
						}catch (Exception e){
							logger.error(String.format("[release][release children cache]%s, %s", clusterId, shardId), e);
						}
                    }
                });
                pathChildrenCache.start();
			} catch (Exception e) {
				logger.error("[observerShardLeader]" + clusterId + " " + shardId, e);
			}
		}else{
			logger.warn("[observerShardLeader][already watched]{}, {}", clusterId, shardId);
		}
	}

	private LockInternalsSorter sorter = new LockInternalsSorter() {
		@Override
		public String fixForSorting(String str, String lockName) {
			return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
		}
	};

	protected void updateShardLeader(String leaderLatchPath, List<ChildData> childrenData, String clusterId, String shardId){

		List<String> childrenPaths = new LinkedList<>();
		childrenData.forEach(childData -> childrenPaths.add(childData.getPath()));

		logger.info("[updateShardLeader]{}, {}, {}", clusterId, shardId, childrenPaths);

		List<String> sortedChildren = LockInternals.getSortedChildren("latch-", sorter, childrenPaths);

		List<KeeperMeta> survivalKeepers = new ArrayList<>(childrenData.size());

		for(String path : sortedChildren){
			for(ChildData childData : childrenData){
				if(path.equals(childData.getPath())){
					String data = new String(childData.getData());
					KeeperMeta keeper = JsonCodec.INSTANCE.decode(data, KeeperMeta.class);
					survivalKeepers.add(keeper);
					break;
				}
			}
		}

		if(survivalKeepers.size() != childrenData.size()){
			throw new IllegalStateException(String.format("[children data not equal with survival keepers]%s, %s", childrenData, survivalKeepers));
		}

		KeeperActiveElectAlgorithm klea = keeperActiveElectAlgorithmManager.get(clusterId, shardId);
		KeeperMeta activeKeeper = klea.select(clusterId, shardId, survivalKeepers);
		currentMetaManager.setSurviveKeepers(clusterId, shardId, survivalKeepers, activeKeeper);
	}

	@Override
	protected void handleClusterModified(ClusterMetaComparator comparator) {

		String clusterId = comparator.getCurrent().getId();

		for(ShardMeta shardMeta : comparator.getAdded()){
			try {
				observerShardLeader(clusterId, shardMeta.getId());
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

	public void setKeeperActiveElectAlgorithmManager(
			KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager) {
		this.keeperActiveElectAlgorithmManager = keeperActiveElectAlgorithmManager;
	}
}
