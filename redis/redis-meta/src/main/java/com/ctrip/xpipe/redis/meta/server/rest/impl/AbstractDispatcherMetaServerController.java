package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SLOT_STATE;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.impl.MultiMetaServer;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.rest.exception.MovingTargetException;
import com.ctrip.xpipe.redis.meta.server.rest.exception.UnfoundAliveSererException;
import com.ctrip.xpipe.rest.ForwardType;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;

import javax.servlet.http.HttpServletRequest;
import java.util.LinkedList;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */

public class AbstractDispatcherMetaServerController extends AbstractController{
	
	protected static final String MODEL_META_SERVER = "MODEL_META_SERVER";
	
	@Autowired
	public MetaServer currentMetaServer;
	
	@Autowired
	private SlotManager slotManager;
	
	@Autowired
	public ClusterServers<MetaServer> servers;

	@Autowired
	public DcMetaCache dcMetaCache;
	
	@ModelAttribute
	public void populateModel(@PathVariable final String clusterId, 
			@RequestHeader(name = MetaServerService.HTTP_HEADER_FOWRARD, required = false) ForwardInfo forwardInfo, Model model, HttpServletRequest request){

		if(forwardInfo != null){
			logger.info("[populateModel]{},{}", clusterId, forwardInfo);
		}

		Long clusterDbId = dcMetaCache.clusterId2DbId(clusterId);
		MetaServer metaServer = getMetaServer(clusterId, clusterDbId, forwardInfo, request.getRequestURI());
		if(metaServer == null){
			throw new UnfoundAliveSererException(clusterId, slotManager.getServerIdByKey(clusterDbId), currentMetaServer.getServerId());
		}
		model.addAttribute(MODEL_META_SERVER, metaServer);
		if(forwardInfo != null){
			model.addAttribute(forwardInfo);
		}
	}
	
	private MetaServer getMetaServer(String clusterId, Long clusterDbId, ForwardInfo forwardInfo, String uri) {

		int slotId = slotManager.getSlotIdByKey(clusterDbId);
		SlotInfo slotInfo = slotManager.getSlotInfo(slotId);
		
		if(forwardInfo != null && forwardInfo.getType() == ForwardType.MOVING){

			if(!(slotInfo.getSlotState() == SLOT_STATE.MOVING  && slotInfo.getToServerId() == currentMetaServer.getServerId())){
				throw new MovingTargetException(forwardInfo, currentMetaServer.getServerId(), slotInfo, clusterId, slotId);
			}
			logger.info("[getMetaServer][use current server]");
			return currentMetaServer;
		}

		if(forwardInfo != null && forwardInfo.getType() == ForwardType.MULTICASTING){
			logger.info("[multicast message][do now]");
			return currentMetaServer;
		}
		
		META_SERVER_SERVICE service = META_SERVER_SERVICE.fromPath(uri);
		Integer serverId = slotManager.getServerIdByKey(clusterDbId);
		if(serverId == null){
			throw new IllegalStateException("clusterDbId:" + clusterDbId + ", unfound server");
		}
		
		if(service.getForwardType() == ForwardType.MULTICASTING){
			
			logger.info("[getMetaServer][multi casting]{}, {}, {}", clusterId, forwardInfo, uri);
			Set<MetaServer> allServers =  servers.allClusterServers();
			MetaServer current = servers.getClusterServer(serverId);
			allServers.remove(current);
			
			return MultiMetaServer.newProxy(current, new LinkedList<>(allServers));
		}else if(service.getForwardType() == ForwardType.FORWARD){
			
			return servers.getClusterServer(serverId);
		}else{
			throw new IllegalStateException("service type can not be:" + service.getForwardType());
		}
	}

}
