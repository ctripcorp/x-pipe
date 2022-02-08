package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.impl.RemoteMetaServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.context.request.async.DeferredResult;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * dispatch service to proper server
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
@RestController
@RequestMapping(META_SERVER_SERVICE.PATH.PATH_PREFIX)
public class DispatcherMetaServerController extends AbstractDispatcherMetaServerController{

	@Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
	private ExecutorService executors;

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CLUSTER_CHANGE, method = RequestMethod.POST,  consumes = MediaType.APPLICATION_JSON_VALUE)
	public void clusterAdded(@PathVariable String clusterId, @RequestBody ClusterMeta clusterMeta, 
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {

		metaServer.clusterAdded(clusterMeta, forwardInfo.clone());
	}
	
	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CLUSTER_CHANGE, method = RequestMethod.PUT,  consumes = MediaType.APPLICATION_JSON_VALUE)
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

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_UPSTREAM_PEER_CHANGE, method = RequestMethod.PUT)
	public void upstreamPeerChange(@PathVariable String dcId, @PathVariable String clusterId, @PathVariable String shardId,
									@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {

		logger.debug("[upstreamPeerChange]{},{}", clusterId, shardId);
		metaServer.upstreamPeerChange(dcId, clusterId, shardId, forwardInfo);
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.GET_ACTIVE_KEEPER, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_VALUE)
	public DeferredResult<KeeperMeta> getActiveKeeper(@PathVariable String clusterId, @PathVariable String shardId,
			@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {
		
		logger.debug("[getActiveKeeper]{},{},{},{}", clusterId, shardId);
		return createDeferredResult(new Function<MetaServer, KeeperMeta >() {
			@Override
			public KeeperMeta apply(MetaServer metaServer) {
				return metaServer.getActiveKeeper(clusterId, shardId, forwardInfo);
			}
		}, metaServer);
	}

	@GetMapping(path = META_SERVER_SERVICE.PATH.GET_PEER_MASTER, produces= MediaType.APPLICATION_JSON_VALUE)
	public DeferredResult<RedisMeta> getPeerMaster(@PathVariable String clusterId, @PathVariable String shardId,
													  @ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {

		logger.debug("[getPeerMaster] {},{}", clusterId, shardId);
		return createDeferredResult(new Function<MetaServer, RedisMeta>() {
			@Override
			public RedisMeta apply(MetaServer metaServer) {
				return metaServer.getCurrentCRDTMaster(clusterId, shardId, forwardInfo);
			}
		}, metaServer);
	}

	@GetMapping(path = META_SERVER_SERVICE.PATH.PATH_GET_CURRENT_MASTER, produces = MediaType.APPLICATION_JSON_VALUE)
	public DeferredResult<RedisMeta> getCurrentMaster(@PathVariable String clusterId, @PathVariable String shardId,
											  @ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer) {
		logger.debug("[getCurrentMaster] {},{}", clusterId, shardId);
		return createDeferredResult(new Function<MetaServer, RedisMeta>() {
			@Override
			public RedisMeta apply(MetaServer metaServer) {
				return metaServer.getCurrentMaster(clusterId, shardId, forwardInfo);
			}
		}, metaServer);
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CHANGE_PRIMARY_DC_CHECK, method = RequestMethod.GET, produces= MediaType.APPLICATION_JSON_VALUE)
	public DeferredResult<PrimaryDcCheckMessage> changePrimaryDcCheck(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable String newPrimaryDc,
																	 @ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){
		
		logger.info("[changePrimaryDcCheck]{}, {}, {}", clusterId, shardId, newPrimaryDc);
		return createDeferredResult(new Function<MetaServer, PrimaryDcCheckMessage >() {
			@Override
			public PrimaryDcCheckMessage apply(MetaServer metaServer) {
				return metaServer.changePrimaryDcCheck(clusterId, shardId, newPrimaryDc, forwardInfo);
			}
		}, metaServer);
	}

	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_MAKE_MASTER_READONLY, method = RequestMethod.PUT, produces= MediaType.APPLICATION_JSON_VALUE)
	public DeferredResult<MetaServerConsoleService.PreviousPrimaryDcMessage> makeMasterReadOnly(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable boolean readOnly,
																				@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){
		
		logger.info("[makeMasterReadOnly]{}, {}, {}", clusterId, shardId, readOnly);
		return createDeferredResult(new Function<MetaServer, MetaServerConsoleService.PreviousPrimaryDcMessage >() {
			@Override
			public MetaServerConsoleService.PreviousPrimaryDcMessage apply(MetaServer metaServer) {
				return metaServer.makeMasterReadOnly(clusterId, shardId, readOnly, forwardInfo);
			}
		}, metaServer);
	}
	
	@RequestMapping(path = META_SERVER_SERVICE.PATH.PATH_CHANGE_PRIMARY_DC, method = RequestMethod.PUT,
			produces= MediaType.APPLICATION_JSON_VALUE,
			consumes = MediaType.APPLICATION_JSON_VALUE)
	public DeferredResult<PrimaryDcChangeMessage> doChangePrimaryDc(@PathVariable String clusterId, @PathVariable String shardId, @PathVariable String newPrimaryDc,
													@RequestBody(required = false) MetaServerConsoleService.PrimaryDcChangeRequest request,
													@ModelAttribute ForwardInfo forwardInfo, @ModelAttribute(MODEL_META_SERVER) MetaServer metaServer){

		logger.info("[doChangePrimaryDc]{}, {}, {}, {}", clusterId, shardId, newPrimaryDc, request);
		return createDeferredResult(new Function<MetaServer, PrimaryDcChangeMessage>() {
			@Override
			public PrimaryDcChangeMessage apply(MetaServer metaServer) {
				return metaServer.doChangePrimaryDc(clusterId, shardId, newPrimaryDc, request, forwardInfo);
			}
		}, metaServer);
	}

	private <T> DeferredResult<T> createDeferredResult(Function<MetaServer, T> function, MetaServer metaServer) {
		DeferredResult<T> response = new DeferredResult<>();
		if (metaServer instanceof RemoteMetaServer) {
			executors.execute(new Runnable() {
				@Override
				public void run() {
					try {
						response.setResult(function.apply(metaServer));
					} catch (RestClientException restException) {
						XpipeException outerException = new XpipeException(restException.getMessage(), restException);
						outerException.setOnlyLogMessage(true);
						response.setErrorResult(outerException);
					} catch (Exception e) {
						response.setErrorResult(e);
					}
				}
			});
		} else {
			response.setResult(function.apply(metaServer));
		}
		return response;
	}

}
