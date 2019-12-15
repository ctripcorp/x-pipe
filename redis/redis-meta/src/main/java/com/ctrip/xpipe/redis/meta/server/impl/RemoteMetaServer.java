package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.AbstractRemoteClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.rest.exception.CircularForwardException;
import com.ctrip.xpipe.rest.ForwardType;
import org.springframework.http.*;

import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class RemoteMetaServer extends AbstractRemoteClusterServer implements MetaServer{

	private ExecutorService executors;
	private String changeClusterPath;
	private String upstreamChangePath;
	private String getActiveKeeperPath;
	private String changePrimaryDcCheckPath;
	private String makeMasterReadonlyPath;
	private String changePrimaryDcPath;
	
	public RemoteMetaServer(int currentServerId, int serverId, ExecutorService executors) {
		super(currentServerId, serverId);
		this.executors = executors;
	}
	
	public RemoteMetaServer(int currentServerId, int serverId, ClusterServerInfo clusterServerInfo, ExecutorService executors) {
		super(currentServerId, serverId, clusterServerInfo);
		this.executors = executors;

		if(getHttpHost() != null){
			changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(getHttpHost());
			upstreamChangePath = META_SERVER_SERVICE.UPSTREAM_CHANGE.getRealPath(getHttpHost());
			getActiveKeeperPath = META_SERVER_SERVICE.GET_ACTIVE_KEEPER.getRealPath(getHttpHost());
			changePrimaryDcCheckPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(getHttpHost());
			makeMasterReadonlyPath = META_SERVER_SERVICE.MAKE_MASTER_READONLY.getRealPath(getHttpHost());
			changePrimaryDcPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(getHttpHost());
		}
	}

	@Override
	public CommandFuture<KeeperMeta> getActiveKeeper(String clusterId, String shardId, ForwardInfo forwardInfo){
	
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.debug("[getActiveKeeper][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);
		return new AbstractCommand<KeeperMeta>() {

			@Override
			public String getName() {
				return getClass().getSimpleName() + "-getActiveKeeper";
			}

			@Override
			protected void doExecute() throws Exception {
				HttpEntity<Void> entity = new HttpEntity<>(headers);
				ResponseEntity<KeeperMeta> response = restTemplate
						.exchange(getActiveKeeperPath, HttpMethod.GET, entity, KeeperMeta.class, clusterId, shardId);
				future().setSuccess(response.getBody());
			}

			@Override
			protected void doReset() {

			}
		}.execute(executors);
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clusterAdded(ClusterMeta clusterMeta, ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CLUSTER_CHANGE.getForwardType());
		logger.info("[clusterAdded][forward]{},{}--> {}", clusterMeta.getId(), forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(clusterMeta, headers);
		restTemplate.exchange(changeClusterPath, HttpMethod.POST, entity, String.class, clusterMeta.getId());
	
	}

	@Override
	public void clusterModified(ClusterMeta clusterMeta, ForwardInfo forwardInfo) {

		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CLUSTER_CHANGE.getForwardType());
		logger.info("[clusterModified][forward]{},{} --> {}", clusterMeta.getId(), forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(clusterMeta, headers);
		restTemplate.exchange(changeClusterPath, HttpMethod.PUT, entity, String.class, clusterMeta.getId());
		
	}

	@Override
	public void clusterDeleted(String clusterId, ForwardInfo forwardInfo) {

		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CLUSTER_CHANGE.getForwardType());
		logger.info("[clusterDeleted][forward]{},{} --> {}", clusterId, forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
		restTemplate.exchange(changeClusterPath, HttpMethod.DELETE, entity, String.class, clusterId);
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String ip, int port, ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.UPSTREAM_CHANGE.getForwardType());
		logger.info("[updateUpstream][forward]{},{},{}:{}, {}--> {}", clusterId, shardId, ip, port, forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
		restTemplate.exchange(upstreamChangePath, HttpMethod.PUT, entity, String.class, clusterId, shardId, ip, port);
		
	}
	
	@Override
	public CommandFuture<PrimaryDcCheckMessage> changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc,
																	ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getForwardType());
		logger.info("[changePrimaryDcCheck][forward]{},{},{}, {}--> {}", clusterId, shardId, newPrimaryDc, forwardInfo, this);
		return new AbstractCommand<PrimaryDcCheckMessage>() {

			@Override
			public String getName() {
				return getClass().getSimpleName() + "-changePrimaryDcCheck";
			}

			@Override
			protected void doExecute() throws Exception {
				HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
				ResponseEntity<PrimaryDcCheckMessage> response = restTemplate
						.exchange(changePrimaryDcCheckPath, HttpMethod.GET, entity,
								PrimaryDcCheckMessage.class, clusterId, shardId, newPrimaryDc);
				future().setSuccess(response.getBody());
			}

			@Override
			protected void doReset() {

			}
		}.execute(executors);
	}

	@Override
	public CommandFuture<MetaServerConsoleService.PreviousPrimaryDcMessage> makeMasterReadOnly(String clusterId, String shardId, boolean readOnly, ForwardInfo forwardInfo) {

		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.MAKE_MASTER_READONLY.getForwardType());
		logger.info("[makeMasterReadOnly][forward]{},{},{}, {}--> {}", clusterId, shardId, readOnly, forwardInfo, this);
		return new AbstractCommand<MetaServerConsoleService.PreviousPrimaryDcMessage>() {

			@Override
			public String getName() {
				return getClass().getSimpleName() + "-makeMasterReadOnly";
			}

			@Override
			protected void doExecute() throws Exception {
				HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
				ResponseEntity<MetaServerConsoleService.PreviousPrimaryDcMessage> response = restTemplate
						.exchange(makeMasterReadonlyPath, HttpMethod.PUT, entity,
								MetaServerConsoleService.PreviousPrimaryDcMessage.class, clusterId, shardId, readOnly);
				future().setSuccess(response.getBody());
			}

			@Override
			protected void doReset() {

			}
		}.execute(executors);
    }

	@Override
	public CommandFuture<PrimaryDcChangeMessage> doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc
			, MetaServerConsoleService.PrimaryDcChangeRequest request, ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getForwardType());
		logger.info("[doChangePrimaryDc][forward]{},{},{}, {}--> {}", clusterId, shardId, newPrimaryDc, forwardInfo, this);
		return new AbstractCommand<PrimaryDcChangeMessage>() {

			@Override
			public String getName() {
				return getClass().getSimpleName() + "-doChangePrimaryDc";
			}

			@Override
			protected void doExecute() throws Exception {
				headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);

				HttpEntity<MetaServerConsoleService.PrimaryDcChangeRequest> entity = new HttpEntity<>(request, headers);
				ResponseEntity<PrimaryDcChangeMessage> response = restTemplate.exchange(changePrimaryDcPath, HttpMethod.PUT,
						entity, PrimaryDcChangeMessage.class, clusterId, shardId, newPrimaryDc);
				future().setSuccess(response.getBody());
			}

			@Override
			protected void doReset() {

			}
		}.execute(executors);
	}


	private HttpHeaders checkCircularAndGetHttpHeaders(ForwardInfo forwardInfo, ForwardType forwardType) {
		
		checkCircular(forwardInfo);

		if(forwardInfo == null){
			forwardInfo = new ForwardInfo(forwardType);
		}else{
			forwardInfo.setType(forwardType);
		}
		forwardInfo.addForwardServers(getCurrentServerId());	
		
		HttpHeaders headers = new HttpHeaders();
		headers.add(MetaServerService.HTTP_HEADER_FOWRARD, Codec.DEFAULT.encode(forwardInfo));
		return headers;
	}

	private HttpHeaders checkCircularAndGetHttpHeaders(ForwardInfo forwardInfo) {
		
		return checkCircularAndGetHttpHeaders(forwardInfo, ForwardType.FORWARD);
	}
	
	private void checkCircular(ForwardInfo forwardInfo) {
		if(forwardInfo != null && forwardInfo.hasServer(getCurrentServerId())){
			throw new CircularForwardException(forwardInfo, getCurrentServerId());
		}
	}

	@Override
	public String getCurrentMeta() {
		return null;
	}

}
