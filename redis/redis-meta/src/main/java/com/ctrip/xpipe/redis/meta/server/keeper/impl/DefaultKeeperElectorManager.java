package com.ctrip.xpipe.redis.meta.server.keeper.impl;


import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.MetaServerEventsHandler;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.exception.ZkException;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperElectorManager;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperLeaderElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaServerMetaManager;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
@Component
public class DefaultKeeperElectorManager extends AbstractLifecycleObservable implements KeeperElectorManager, Observer, TopElement{
	
	@Autowired
	private ZkClient zkClient;
	
	private Map<Pair<String, String>, KeeperMeta> aliveKeeper = new ConcurrentHashMap<>();
	
	private Set<String> watchedClusters = new HashSet<>();
	
	@Autowired
	private CurrentMetaServerMetaManager currentMetaServerMetaManager; 

	@Autowired
	private MetaServerEventsHandler metaServerEventsHandler;
	
	@Autowired
	private CurrentClusterServer currentClusterServer;
	
	@Override
	protected void doInitialize() throws Exception {
		
		super.doInitialize();
		currentMetaServerMetaManager.addObserver(this);
	}
	
	@Override
	public List<KeeperMeta> getAllAliveKeepers(String clusterId, String shardId) {

		final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(clusterId, shardId);
		List<KeeperMeta> result = new LinkedList<>();
		CuratorFramework client = zkClient.get();
		
		try {
			for(String child : client.getChildren().forPath(leaderLatchPath)){
				byte []data = client.getData().forPath(leaderLatchPath + "/" + child);
				result.add(Codec.DEFAULT.decode(data, KeeperMeta.class));
			}
		} catch (Exception e) {
			throw new ZkException("[getAllAliveKeepers]" + clusterId + "," + shardId, e);
		}
		return result;
	}

	@Override
	public KeeperMeta getActive(String clusterId, String shardId) {
		return aliveKeeper.get(new Pair<String, String>(clusterId, shardId));
	}

	@Override
	public void watchCluster(ClusterMeta clusterMeta) throws Exception {

		logger.info("[observeCluster]{}", clusterMeta);
		watchedClusters.add(clusterMeta.getId());
		observeLeader(clusterMeta);
	}
	
	@Override
	public void unwatchCluster(ClusterMeta clusterMeta) throws Exception {
		
		logger.info("[unwatchCluster]{}", clusterMeta);
		watchedClusters.remove(clusterMeta.getId());
		
		Set<Pair<String, String>> toDelete = new HashSet<>();
		for(Pair<String, String> key : aliveKeeper.keySet()){
			if(key.getKey().equals(clusterMeta.getId())){
				toDelete.add(key);
			}
		}
		
		for(Pair<String, String> key : toDelete){
			aliveKeeper.remove(key);
		}
		
	}

	private void observeLeader(final ClusterMeta cluster) throws Exception {

		logger.info("[observeLeader]{}", cluster.getId());
		
		final CuratorFramework client = zkClient.get();
		
		for (final ShardMeta shard : cluster.getShards().values()) {
			
			final String leaderLatchPath = MetaZkConfig.getKeeperLeaderLatchPath(cluster.getId(), shard.getId());
			
			EnsurePath ensure = client.newNamespaceAwareEnsurePath(leaderLatchPath);
			ensure.ensure(client.getZookeeperClient());

			logger.info("[observeLeader]{}, {} , ({})", cluster.getId(), shard.getId(), currentClusterServer.getServerId());
			List<String> children = client.getChildren().usingWatcher(new CuratorWatcher() {

				@Override
				public void process(WatchedEvent event) throws Exception {
					
					if(!watchedClusters.contains(cluster.getId())){
						logger.info("[cluster clean watch]{}", cluster.getId());
						return;
					}
					logger.info("[process]{}, {}, {}",  event, this.hashCode(), currentClusterServer.getServerId());
					List<String> children = client.getChildren().usingWatcher(this).forPath(leaderLatchPath);
					updateShardLeader(children, leaderLatchPath, cluster.getId(),shard.getId());
				}
			}).forPath(leaderLatchPath);

			updateShardLeader(children, leaderLatchPath, cluster.getId(), shard.getId());
		}
	}

	private void updateShardLeader(List<String> children, String leaderLatchPath, String clusterId, String shardId) throws Exception {

		logger.info("[updateShardLeader]{}, {}, {}, {}", children, leaderLatchPath, clusterId, shardId);
		KeeperLeaderElectAlgorithm klea = new DefaultLeaderElectAlgorithm();
		KeeperMeta keeper = klea.select(leaderLatchPath, children, zkClient.get());

		if(keeper == null){
			logger.warn("[updateShardLeader][leader null]{}, {}, {}", children, clusterId, shardId);
			metaServerEventsHandler.noneActiveElected(clusterId, shardId);
			return;
		}
		aliveKeeper.put(new Pair<String, String>(clusterId, shardId), keeper);
		metaServerEventsHandler.keeperActiveElected(clusterId, shardId, keeper);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void update(Object args, Observable observable) {
		
		if(args instanceof NodeAdded<?>){
			NodeAdded<ClusterMeta> nodeAdded = (NodeAdded<ClusterMeta>) args;
			try {
				watchCluster(nodeAdded.getNode());
			} catch (Exception e) {
				logger.error("[update]{}", e);
			}
		}

		if(args instanceof NodeDeleted<?>){
			
			NodeDeleted<ClusterMeta> nodeDeleted = (NodeDeleted<ClusterMeta>) args;
			try {
				unwatchCluster(nodeDeleted.getNode());
			} catch (Exception e) {
				logger.error("[update]{}", e);
			}
		}
	}

}
