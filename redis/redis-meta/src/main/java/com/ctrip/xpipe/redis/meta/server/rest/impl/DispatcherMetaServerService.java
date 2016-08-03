package com.ctrip.xpipe.redis.meta.server.rest.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.rest.exception.UnfoundAliveSererException;

/**
 * dispatch service to proper server
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@RestController
@RequestMapping("/api/v1")
public class DispatcherMetaServerService extends AbstractMetaServerService{
	
	@Autowired
	private SlotManager slotManager;

	@Autowired
	public ClusterServers<MetaServer> servers;

	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta) {
		MetaServer metaServer = getMetaServer(clusterId);
		if(metaServer == null){
			throw new UnfoundAliveSererException(clusterId, slotManager.getServerIdByKey(clusterId));
		}
		metaServer.ping(clusterId, shardId, keeperInstanceMeta);
	}

	private MetaServer getMetaServer(String clusterId) {
		
		Integer serverId = slotManager.getServerIdByKey(clusterId);
		if(serverId == null){
			throw new IllegalStateException("clusterId:" + clusterId + ", unfound server");
		}
		return servers.getClusterServer(serverId);
	}

	@Override
	public void addKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		
	}

	@Override
	public void removeKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		
	}

	@Override
	public void setKeepers(String clusterId, String shardId, KeeperMeta keeperMeta, List<KeeperMeta> keeperMetas) {
		
	}

	@Override
	public void clusterChanged(String clusterId) {
		
	}

	@Override
	public DcMeta getDynamicInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MetaServerMeta> getAllMetaServers() {
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService#getAllKeepersByKeeperContainer(com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta)
	 */
	@Override
	public List<KeeperTransMeta> getAllKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public List<KeeperTransMeta> getKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		// TODO Auto-generated method stub
		return null;
	}
}
