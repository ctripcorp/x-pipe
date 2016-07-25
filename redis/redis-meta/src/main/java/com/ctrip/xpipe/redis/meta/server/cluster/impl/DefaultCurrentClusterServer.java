package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.zk.ZkClient;

/**
 * @author wenchao.meng
 *
 * Jul 25, 2016
 */
@Component
public class DefaultCurrentClusterServer extends AbstractLifecycle implements CurrentClusterServer, TopElement{
	
	@Autowired
	private ZkClient zkClient;
	
	@Autowired
	private MetaServerConfig config;
	
	private int currentServerId;
	
	private String serverPath;
	
	@Override
	protected void doInitialize() throws Exception {
		
		this.currentServerId = config.getMetaServerId();
		serverPath = MetaZkConfig.getMetaServerRegisterPath() + "/" + currentServerId;
		
	}
	
	@Override
	protected void doStart() throws Exception {
		
		CuratorFramework client = zkClient.get();		

		if(client.checkExists().forPath(serverPath) != null){
			
			ClusterServerInfo info = Codec.DEFAULT.decode(client.getData().forPath(serverPath), ClusterServerInfo.class);
			if(!info.equals(getClusterInfo())){
				throw new IllegalStateException("serverId:" + currentServerId + " already exists!");
			}
			deleteServerPath();
		}
		
		logger.info("[doStart][createServerPathCreated]{}", serverPath);
		client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(serverPath, Codec.DEFAULT.encodeAsBytes(getClusterInfo()));
	}

	private void deleteServerPath() throws Exception {
		
		logger.info("[deleteServerPath]{}", serverPath);
		CuratorFramework client = zkClient.get();
		client.delete().forPath(serverPath);
		
		
	}

	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	protected void doStop() throws Exception {
	
		deleteServerPath();
	}
	
	public void setZkClient(ZkClient zkClient) {
		this.zkClient = zkClient;
	}

	public void setConfig(MetaServerConfig config) {
		this.config = config;
	}

	@Override
	public int getServerId() {
		return config.getMetaServerId();
	}

	@Override
	public ClusterServerInfo getClusterInfo() {
		return new ClusterServerInfo(config.getMetaServerIp(), config.getMetaServerPort());
	}

}
