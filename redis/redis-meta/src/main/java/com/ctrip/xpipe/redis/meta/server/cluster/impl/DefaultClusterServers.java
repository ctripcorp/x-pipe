package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.observer.NodeModified;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.RemoteClusterServerFactory;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
@Component
public class DefaultClusterServers extends AbstractLifecycleObservable implements ClusterServers, TopElement, CuratorWatcher{
	
	private Map<Integer, ClusterServer> servers = new ConcurrentHashMap<>();
	
	@Autowired
	private MetaServerConfig  metaServerConfig;
	
	@Autowired
	private ZkClient zkClient;
		
	@Autowired
	private CurrentClusterServer currentServer;
	
	@Autowired
	private RemoteClusterServerFactory remoteClusterServerFactory;
	
	@Override
	protected void doInitialize() throws Exception {
	
	}
	
	@Override
	protected void doStart() throws Exception {
		
		int currentServerId = currentServer.getServerId();
		servers.put(currentServerId, currentServer);

		childrenChanged();
		watchServers();
		
	}

	private void watchServers() throws Exception {
		zkClient.get().getChildren().usingWatcher(this).forPath(MetaZkConfig.getMetaServerRegisterPath());		
	}

	@Override
	public ClusterServer currentClusterServer() {
		
		return currentServer;
	}

	@Override
	public ClusterServer getClusterServer(int serverId) {
		return servers.get(serverId);
	}

	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	public void process(WatchedEvent event) throws Exception {
		
		watchServers();
		childrenChanged();
	}

	private void childrenChanged() throws Exception {

		CuratorFramework client = zkClient.get();
		List<String> children = client.getChildren().forPath(MetaZkConfig.getMetaServerRegisterPath());
		
		Set<Integer> currentServers = new HashSet<>();
		for(String child : children){
			
			int serverId = Integer.parseInt(child);
			byte []data = client.getData().forPath(MetaZkConfig.getMetaServerRegisterPath() + "/" + child);
			ClusterServerInfo  info = Codec.DEFAULT.decode(data, ClusterServerInfo.class);
			
			currentServers.add(serverId);
			if(serverId == metaServerConfig.getMetaServerId()){
				continue;
			}
			
			ClusterServer server = servers.get(serverId);
			if(server == null){
				logger.info("[childrenChanged][createNew]{}{}", child, info);
				ClusterServer remoteServer = remoteClusterServerFactory.createClusterServer(serverId, info);
				servers.put(serverId, remoteServer);
				serverAdded(remoteServer);
			}else{
				if(!info.equals(server.getClusterInfo())){
					
					logger.info("[childrenChanged][clusterInfoChanged]{}{}", child, info, server.getClusterInfo());
					ClusterServer newServer = remoteClusterServerFactory.createClusterServer(serverId, info);
					servers.put(serverId, newServer);
					serverChanged(server, newServer);
					
				}
			}
		}

		
		for(Integer old : servers.keySet()){
			if(!currentServers.contains(old)){
				
				ClusterServer serverInfo = servers.remove(old);
				logger.info("[childrenChanged][remote not exist]{}, {}", old, serverInfo);
				remoteDelted(serverInfo);
				
			}
		}
	}

	
	private void remoteDelted(ClusterServer serverInfo) {
		notifyObservers(new NodeDeleted<ClusterServer>(serverInfo));
	}

	private void serverChanged(ClusterServer oldServer, ClusterServer newServer) {
		notifyObservers(new NodeModified<ClusterServer>(oldServer, newServer));
	}

	private void serverAdded(ClusterServer remoteServer) {
		notifyObservers(new NodeAdded<ClusterServer>(remoteServer));
	}

	public void setMetaServerConfig(MetaServerConfig metaServerConfig) {
		this.metaServerConfig = metaServerConfig;
	}
	
	public void setZkClient(ZkClient zkClient) {
		this.zkClient = zkClient;
	}
	
	public void setCurrentServer(CurrentClusterServer currentServer) {
		this.currentServer = currentServer;
	}
	
	public void setRemoteClusterServerFactory(RemoteClusterServerFactory remoteClusterServerFactory) {
		this.remoteClusterServerFactory = remoteClusterServerFactory;
	}

	@Override
	public Set<ClusterServer> allClusterServers() {
		return new HashSet<>(servers.values());
	}

	@Override
	public void refresh() throws Exception {
		childrenChanged();
	}

	@Override
	public boolean exist(int serverId) {
		return servers.get(serverId) != null;
	}
	
}
