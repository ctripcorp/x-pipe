package com.ctrip.xpipe.redis.meta.server.rest.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SLOT_STATE;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardType;
import com.ctrip.xpipe.redis.meta.server.rest.exception.MovingTargetException;
import com.ctrip.xpipe.redis.meta.server.rest.exception.UnfoundAliveSererException;
import com.ctrip.xpipe.spring.AbstractController;

/**
 * dispatch service to proper server
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@RestController
@RequestMapping(MetaServerService.PATH_PREFIX)
public class DispatcherMetaServerController extends AbstractController{
	
	private static final String MODEL_META_SERVER = "MODEL_META_SERVER";
	
	@Autowired
	public MetaServer currentMetaServer;
	
	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	public ClusterServers<MetaServer> servers;
	
	@ModelAttribute
	public void populateModel(@PathVariable final String clusterId, 
			@RequestHeader(name = MetaServerService.HTTP_HEADER_FOWRARD, required = false) ForwardInfo forwardInfo, Model model){

		if(forwardInfo != null){
			logger.info("[populateModel]{},{}", clusterId, forwardInfo);
		}
		MetaServer metaServer = getMetaServer(clusterId, forwardInfo);
		if(metaServer == null){
			throw new UnfoundAliveSererException(clusterId, slotManager.getServerIdByKey(clusterId), currentMetaServer.getServerId());
		}
		model.addAttribute(MODEL_META_SERVER, metaServer);
		if(forwardInfo != null){
			model.addAttribute(forwardInfo);
		}
	}
	
	
	@RequestMapping(path = MetaServerKeeperService.PATH_SHARD_STATUS, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public ShardStatus getShardStatus(@PathVariable final String clusterId, @PathVariable final String shardId,
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) throws Exception {
		
		return metaServer.getShardStatus(clusterId, shardId, forwardInfo);
	}


	@RequestMapping(path = MetaServerKeeperService.PATH_PING, method = RequestMethod.POST,  consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public void ping(@PathVariable String clusterId, @PathVariable String shardId, @RequestBody KeeperInstanceMeta keeperInstanceMeta,
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute MetaServer metaServer) {
		
		metaServer.ping(clusterId, shardId, keeperInstanceMeta, forwardInfo);
	}

	private MetaServer getMetaServer(String clusterId, ForwardInfo forwardInfo) {
		
		int slotId = slotManager.getSlotIdByKey(clusterId);
		SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
		
		if(forwardInfo != null && forwardInfo.getType() == ForwardType.MOVING){

			if(!(slotInfo.getSlotState() == SLOT_STATE.MOVING  && slotInfo.getToServerId() == currentMetaServer.getServerId())){
				throw new MovingTargetException(forwardInfo, currentMetaServer.getServerId(), slotInfo, clusterId, slotId);
			}
			logger.info("[getMetaServer][use current server]");
			return currentMetaServer;
		}
		
		Integer serverId = slotManager.getServerIdByKey(clusterId);
		if(serverId == null){
			throw new IllegalStateException("clusterId:" + clusterId + ", unfound server");
		}
		return servers.getClusterServer(serverId);
	}

	@RequestMapping(path = MetaServerConsoleService.PATH_CLUSTER_CHANGE, method = RequestMethod.POST,  consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public void clusterAdded(@PathVariable String clusterId, @RequestBody ClusterMeta clusterMeta, @ModelAttribute ForwardInfo forwardInfo) {
		
		if(forwardInfo != null && forwardInfo.getType() == ForwardType.MULTICASTING){
			logger.info("[clusterAdded][multicast message][do now]");
			currentMetaServer.clusterAdded(clusterMeta, forwardInfo);
			return;
		}
		
		for(MetaServer metaServer : servers.allClusterServers()){
			metaServer.clusterAdded(clusterMeta, forwardInfo.clone());
		}
	}
	
	@RequestMapping(path = MetaServerConsoleService.PATH_CLUSTER_CHANGE, method = RequestMethod.PUT,  consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public void clusterModified(@PathVariable String clusterId, @RequestBody ClusterMeta clusterMeta, @ModelAttribute ForwardInfo forwardInfo) {
		
		if(forwardInfo != null && forwardInfo.getType() == ForwardType.MULTICASTING){
			logger.info("[clusterModified][multicast message][do now]");
			currentMetaServer.clusterModified(clusterMeta, forwardInfo);
			return;
		}
		
		for(MetaServer metaServer : servers.allClusterServers()){
			metaServer.clusterModified(clusterMeta, forwardInfo.clone());
		}
	}

	@RequestMapping(path = MetaServerConsoleService.PATH_CLUSTER_CHANGE, method = RequestMethod.DELETE)
	public void clusterDeleted(@PathVariable String clusterId, @ModelAttribute ForwardInfo forwardInfo) {

		if(forwardInfo != null && forwardInfo.getType() == ForwardType.MULTICASTING){
			logger.info("[clusterDeleted][multicast message][do now]");
			currentMetaServer.clusterDeleted(clusterId, forwardInfo);
			return;
		}
		
		for(MetaServer metaServer : servers.allClusterServers()){
			metaServer.clusterDeleted(clusterId, forwardInfo.clone());
		}
	}

}
