package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.endpoint.HostPort;
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

import static com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE.GET_CURRENT_MASTER;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class RemoteMetaServer extends AbstractRemoteClusterServer implements MetaServer{
	
	private String changeClusterPath;
	private String upstreamChangePath;
	private String upstreamPeerChangePath;
	private String getActiveKeeperPath;
	private String getPeerMasterPath;
	private String getSidsPath;
	private String getCurrentMasterPath;
	private String changePrimaryDcCheckPath;
	private String makeMasterReadonlyPath;
	private String changePrimaryDcPath;

	public RemoteMetaServer(int currentServerId, int serverId) {
		super(currentServerId, serverId);
	}
	
	public RemoteMetaServer(int currentServerId, int serverId, ClusterServerInfo clusterServerInfo) {
		super(currentServerId, serverId, clusterServerInfo);
				
		if(getHttpHost() != null){
			changeClusterPath = META_SERVER_SERVICE.CLUSTER_CHANGE.getRealPath(getHttpHost());
			upstreamChangePath = META_SERVER_SERVICE.UPSTREAM_CHANGE.getRealPath(getHttpHost());
			upstreamPeerChangePath = META_SERVER_SERVICE.UPSTREAM_PEER_CHANGE.getRealPath(getHttpHost());
			getActiveKeeperPath = META_SERVER_SERVICE.GET_ACTIVE_KEEPER.getRealPath(getHttpHost());
			changePrimaryDcCheckPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getRealPath(getHttpHost());
			makeMasterReadonlyPath = META_SERVER_SERVICE.MAKE_MASTER_READONLY.getRealPath(getHttpHost());
			changePrimaryDcPath = META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getRealPath(getHttpHost());
			getPeerMasterPath = META_SERVER_SERVICE.GET_PEER_MASTER.getRealPath(getHttpHost());
			getSidsPath = META_SERVER_SERVICE.GET_SIDS.getRealPath(getHttpHost());
			getCurrentMasterPath = GET_CURRENT_MASTER.getRealPath(getHttpHost());
		}
	}

	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId, ForwardInfo forwardInfo){
	
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.debug("[getActiveKeeper][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);

		HttpEntity<Void> entity = new HttpEntity<>(headers);
		ResponseEntity<KeeperMeta> response = restTemplate.exchange(getActiveKeeperPath, HttpMethod.GET, entity, KeeperMeta.class, clusterId, shardId);
		return response.getBody();
	}

	@Override
	public RedisMeta getCurrentCRDTMaster(String clusterId, String shardId, ForwardInfo forwardInfo) {
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.debug("[getCurrentCRDTMaster][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);

		HttpEntity<Void> entity = new HttpEntity<>(headers);
		ResponseEntity<RedisMeta> response = restTemplate.exchange(getPeerMasterPath, HttpMethod.GET, entity, RedisMeta.class, clusterId, shardId);
		return response.getBody();
	}

	@Override
	public String getSids(String srcDcId, String clusterId, String shardId, ForwardInfo forwardInfo) {
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.debug("[getSids][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);

		HttpEntity<Void> entity = new HttpEntity<>(headers);
		ResponseEntity<String> response = restTemplate.exchange(getSidsPath, HttpMethod.GET, entity, String.class, srcDcId, clusterId, shardId);
		return response.getBody();
	}

	@Override
	public RedisMeta getCurrentMaster(String clusterId, String shardId, ForwardInfo forwardInfo) {
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.debug("[getCurrentMaster][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);

		HttpEntity<Void> entity = new HttpEntity<>(headers);
		ResponseEntity<RedisMeta> response = restTemplate.exchange(getCurrentMasterPath, HttpMethod.GET, entity, RedisMeta.class, clusterId, shardId);
		return response.getBody();
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
	public void upstreamPeerChange(String upstreamDcId, String clusterId, String shardId, ForwardInfo forwardInfo) {
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.UPSTREAM_CHANGE.getForwardType());
		logger.info("[upstreamPeerChange][forward]{},{},{}, {}--> {}", upstreamDcId, clusterId, shardId, forwardInfo, this);

		HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
		restTemplate.exchange(upstreamPeerChangePath, HttpMethod.PUT, entity, String.class, upstreamDcId, clusterId, shardId);
	}
	
	@Override
	public PrimaryDcCheckMessage changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc,
			ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CHANGE_PRIMARY_DC_CHECK.getForwardType());
		logger.info("[changePrimaryDcCheck][forward]{},{},{}, {}--> {}", clusterId, shardId, newPrimaryDc, forwardInfo, this);
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
		ResponseEntity<PrimaryDcCheckMessage> result = restTemplate.exchange(changePrimaryDcCheckPath, HttpMethod.GET, entity, PrimaryDcCheckMessage.class, clusterId, shardId, newPrimaryDc);
		return result.getBody();
	}

	@Override
	public MetaServerConsoleService.PreviousPrimaryDcMessage makeMasterReadOnly(String clusterId, String shardId, boolean readOnly, ForwardInfo forwardInfo) {

		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.MAKE_MASTER_READONLY.getForwardType());
		logger.info("[makeMasterReadOnly][forward]{},{},{}, {}--> {}", clusterId, shardId, readOnly, forwardInfo, this);

		HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
		ResponseEntity<MetaServerConsoleService.PreviousPrimaryDcMessage> result = restTemplate.exchange(makeMasterReadonlyPath, HttpMethod.PUT, entity, MetaServerConsoleService.PreviousPrimaryDcMessage.class, clusterId, shardId, readOnly);
		return result.getBody();
    }

	@Override
	public PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc
			, MetaServerConsoleService.PrimaryDcChangeRequest request, ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, META_SERVER_SERVICE.CHANGE_PRIMARY_DC.getForwardType());
		logger.info("[doChangePrimaryDc][forward]{},{},{}, {}--> {}", clusterId, shardId, newPrimaryDc, forwardInfo, this);
		headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);

		HttpEntity<MetaServerConsoleService.PrimaryDcChangeRequest> entity = new HttpEntity<>(request, headers);
		ResponseEntity<PrimaryDcChangeMessage> resposne = restTemplate.exchange(changePrimaryDcPath, HttpMethod.PUT, 
				entity, PrimaryDcChangeMessage.class, clusterId, shardId, newPrimaryDc);
		return resposne.getBody();
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
