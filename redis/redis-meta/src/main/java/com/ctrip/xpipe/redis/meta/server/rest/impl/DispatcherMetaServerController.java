package com.ctrip.xpipe.redis.meta.server.rest.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.rest.exception.UnfoundAliveSererException;

/**
 * dispatch service to proper server
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@RestController
public class DispatcherMetaServerController{
	
	@Autowired
	public MetaServer currentMetaServer;
	
	@Autowired
	private SlotManager slotManager;

	@Autowired
	public ClusterServers<MetaServer> servers;

	@RequestMapping(path = MetaServerKeeperService.PATH_PING, method = RequestMethod.POST,  consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public void ping(@PathVariable String clusterId, @PathVariable String shardId, KeeperInstanceMeta keeperInstanceMeta, 
			@RequestHeader(name = MetaServerService.HTTP_HEADER_FOWRARD, required = false) ForwardInfo forwardInfo) {
		
		MetaServer metaServer = getMetaServer(clusterId, forwardInfo);
		if(metaServer == null){
			throw new UnfoundAliveSererException(clusterId, slotManager.getServerIdByKey(clusterId), currentMetaServer.getServerId());
		}
		metaServer.ping(clusterId, shardId, keeperInstanceMeta, forwardInfo);
	}

	private MetaServer getMetaServer(String clusterId, ForwardInfo forwardInfo) {

		Integer serverId = slotManager.getServerIdByKey(clusterId);
		if(serverId == null){
			throw new IllegalStateException("clusterId:" + clusterId + ", unfound server");
		}
		return servers.getClusterServer(serverId);
	}

	public void addKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		
	}

	public void removeKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		
	}

	public void setKeepers(String clusterId, String shardId, KeeperMeta keeperMeta, List<KeeperMeta> keeperMetas) {
		
	}

	public void clusterChanged(String clusterId) {
		
	}

	public DcMeta getDynamicInfo() {
		return null;
	}

	public List<MetaServerMeta> getAllMetaServers() {
		return null;
	}

	public List<KeeperTransMeta> getAllKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		return null;
	}
	
	public List<KeeperTransMeta> getKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		return null;
	}

	@RequestMapping(path = MetaServerKeeperService.PATH_PREFIX + "/slots", method = RequestMethod.GET)
	public Set<Integer> getSlots(){
		
		return slotManager.getSlotsByServerId(currentMetaServer.getServerId());
	}

	@RequestMapping(path = MetaServerKeeperService.PATH_PREFIX + "/debugslots", method = RequestMethod.GET)
	public Map<Integer, SlotInfo> getSlotsInfo(){

		return slotManager.allSlotsInfo();
	}

}
