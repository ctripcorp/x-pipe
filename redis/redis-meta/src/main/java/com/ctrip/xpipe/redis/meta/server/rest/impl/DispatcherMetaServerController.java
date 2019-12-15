package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
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
import org.springframework.web.context.request.async.DeferredResult;

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
		
		logger.debug("[upstreamChange]{},{},{},{}", clusterId, shardId, ip, port);
		metaServer.updateUpstream(clusterId, shardId, ip, port, forwardInfo);
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.GET_ACTIVE_KEEPER, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public DeferredResult<KeeperMeta> getActiveKeeper(@PathVariable String clusterId, @PathVariable String shardId,
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {
		
		logger.debug("[getActiveKeeper]{},{},{},{}", clusterId, shardId);
		return newTransformer(metaServer.getActiveKeeper(clusterId, shardId, forwardInfo)).transform();
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CHANGE_PRIMARY_DC_CHECK, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public DeferredResult<PrimaryDcCheckMessage> changePrimaryDcCheck(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable String newPrimaryDc,
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){
		
		logger.info("[changePrimaryDcCheck]{}, {}, {}", clusterId, shardId, newPrimaryDc);
		return newTransformer(metaServer.changePrimaryDcCheck(clusterId, shardId, newPrimaryDc, forwardInfo)).transform();
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_MAKE_MASTER_READONLY, method = RequestMethod.PUT, produces= MediaType.APPLICATION_JSON_UTF8_VALUE)
	public DeferredResult<MetaServerConsoleService.PreviousPrimaryDcMessage> makeMasterReadOnly(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable boolean readOnly,
																				@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){
		
		logger.info("[makeMasterReadOnly]{}, {}, {}", clusterId, shardId, readOnly);
		return newTransformer(metaServer.makeMasterReadOnly(clusterId, shardId, readOnly, forwardInfo)).transform();
	}
	
	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CHANGE_PRIMARY_DC, method = RequestMethod.PUT,
			produces= MediaType.APPLICATION_JSON_UTF8_VALUE,
			consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public DeferredResult<PrimaryDcChangeMessage> doChangePrimaryDc(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable String newPrimaryDc,
																   @RequestBody(required = false) MetaServerConsoleService.PrimaryDcChangeRequest request,
																   @ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){

		logger.info("[doChangePrimaryDc]{}, {}, {}, {}", clusterId, shardId, newPrimaryDc, request);
		return newTransformer(metaServer.doChangePrimaryDc(clusterId, shardId, newPrimaryDc, request, forwardInfo)).transform();
	}

	private <V> CommandFutureToDeferredResultTransformer<V> newTransformer(CommandFuture<V> future) {
		return new CommandFutureToDeferredResultTransformer<>(future);
	}

	private class CommandFutureToDeferredResultTransformer<T> {

		private CommandFuture<T> future;

		public CommandFutureToDeferredResultTransformer(CommandFuture<T> future) {
			this.future = future;
		}

		public DeferredResult<T> transform() {
			DeferredResult<T> result = new DeferredResult<>();

			future.addListener(new CommandFutureListener<T>() {
				@Override
				public void operationComplete(CommandFuture<T> commandFuture) throws Exception {
					if (commandFuture.isSuccess()) {
						result.setResult(commandFuture.getNow());
					} else {
						result.setErrorResult(commandFuture.cause());
					}
				}
			});
			return result;
		}
	}
}
