package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

/**
 * dispatch service to proper server
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@RestController
@RequestMapping(META_SERVER_SERVICE.PATH.PATH_PREFIX)
public class DispatcherMetaServerController extends AbstractDispatcherMetaServerController{
	

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CLUSTER_CHANGE, method = RequestMethod.POST,  consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public void clusterAdded(@PathVariable String clusterId, @RequestBody ClusterMeta clusterMeta, 
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {

		metaServer.clusterAdded(clusterMeta, forwardInfo.clone());
	}
	
	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CLUSTER_CHANGE, method = RequestMethod.PUT,  consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public void clusterModified(@PathVariable String clusterId, @RequestBody ClusterMeta clusterMeta, 
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {
		
		metaServer.clusterModified(clusterMeta, forwardInfo.clone());
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CLUSTER_CHANGE, method = RequestMethod.DELETE)
	public void clusterDeleted(@PathVariable String clusterId, 
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {

		metaServer.clusterDeleted(clusterId, forwardInfo.clone());
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_UPSTREAM_CHANGE, method = RequestMethod.PUT)
	public void upstreamChange(@PathVariable String clusterId, @PathVariable String shardId, 
			@PathVariable String ip, @PathVariable int port,@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {
		
		logger.info("[upstreamChange]{},{},{},{}", clusterId, shardId, ip, port);
		metaServer.updateUpstream(clusterId, shardId, ip, port, forwardInfo);
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.GET_ACTIVE_KEEPER, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public KeeperMeta getActiveKeeper(@PathVariable String clusterId, @PathVariable String shardId, 
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {
		
		logger.info("[getActiveKeeper]{},{},{},{}", clusterId, shardId);
		return metaServer.getActiveKeeper(clusterId, shardId, forwardInfo);
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CHANGE_PRIMARY_DC_CHECK, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public PrimaryDcCheckMessage changePrimaryDcCheck(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable String newPrimaryDc, 
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){
		
		logger.info("[changePrimaryDcCheck]{}, {}, {}", clusterId, shardId, newPrimaryDc);
		return metaServer.changePrimaryDcCheck(clusterId, shardId, newPrimaryDc, forwardInfo);
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_MAKE_MASTER_READONLY, method = RequestMethod.PUT, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public MetaServerConsoleService.PreviousPrimaryDcMessage makeMasterReadOnly(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable boolean readOnly,
																				@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){
		
		logger.info("[makeMasterReadOnly]{}, {}, {}", clusterId, shardId, readOnly);
		return metaServer.makeMasterReadOnly(clusterId, shardId, readOnly, forwardInfo);
	}
	
	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CHANGE_PRIMARY_DC, method = RequestMethod.PUT,
			produces= MediaType.APPLICATION_JSON_UTF8_VALUE,
			consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public PrimaryDcChangeMessage doChangePrimaryDc(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable String newPrimaryDc,
													@RequestBody(required = false) MetaServerConsoleService.PrimaryDcChangeRequest request,
													@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){

		logger.info("[doChangePrimaryDc]{}, {}, {}, {}", clusterId, shardId, newPrimaryDc, request);
		return metaServer.doChangePrimaryDc(clusterId, shardId, newPrimaryDc, request, forwardInfo);
	}

}
