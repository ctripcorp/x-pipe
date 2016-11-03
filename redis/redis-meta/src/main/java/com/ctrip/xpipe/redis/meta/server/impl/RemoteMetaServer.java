package com.ctrip.xpipe.redis.meta.server.impl;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerService;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.AbstractRemoteClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardType;
import com.ctrip.xpipe.redis.meta.server.rest.exception.CircularForwardException;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class RemoteMetaServer extends AbstractRemoteClusterServer implements MetaServer{
	
	private String pingPath;
	private String getShardStatusPath;
	private String changeClusterPath;
	private String upstreamChangePath;
	private String getActiveKeeperPath;

	public RemoteMetaServer(int currentServerId, int serverId) {
		super(currentServerId, serverId);
	}
	
	public RemoteMetaServer(int currentServerId, int serverId, ClusterServerInfo clusterServerInfo) {
		super(currentServerId, serverId, clusterServerInfo);
		
		pingPath = String.format("%s/%s/%s", getHttpHost(), MetaServerKeeperService.PATH_PREFIX, MetaServerKeeperService.PATH_PING);
		getShardStatusPath = String.format("%s/%s/%s", getHttpHost(), MetaServerKeeperService.PATH_PREFIX, MetaServerKeeperService.PATH_SHARD_STATUS);
		changeClusterPath = String.format("%s/%s/%s", getHttpHost(), MetaServerConsoleService.PATH_PREFIX, MetaServerConsoleService.PATH_CLUSTER_CHANGE);
		upstreamChangePath = String.format("%s/%s/%s", getHttpHost(), MetaServerConsoleService.PATH_PREFIX, MetaServerMultiDcService.PATH_UPSTREAM_CHANGE);
		getActiveKeeperPath = String.format("%s/%s/%s", getHttpHost(), MetaServerConsoleService.PATH_PREFIX, MetaServerService.GET_ACTIVE_KEEPER);
	}

	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId, ForwardInfo forwardInfo){
	
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.info("[getActiveKeeper][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);

		HttpEntity<Void> entity = new HttpEntity<>(headers);
		ResponseEntity<KeeperMeta> response = restTemplate.exchange(getActiveKeeperPath, HttpMethod.GET, entity, KeeperMeta.class, clusterId, shardId);
		return response.getBody();
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public KeeperMeta getUpstreamKeeper(String clusterId, String shardId) throws Exception {
		throw new UnsupportedOperationException();
	}


	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta, ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.info("[ping][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);

		HttpEntity<KeeperInstanceMeta> entity = new HttpEntity<KeeperInstanceMeta>(keeperInstanceMeta, headers);
		restTemplate.postForObject(pingPath, entity, String.class, clusterId, shardId);
	}


	@Override
	public ShardStatus getShardStatus(String clusterId, String shardId, ForwardInfo forwardInfo) throws Exception {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.info("[pgetShardStatusing][forward]{},{},{} --> {}", clusterId, shardId, forwardInfo, this);

		HttpEntity<Void> entity = new HttpEntity<>(headers);
		ResponseEntity<ShardStatus> response = restTemplate.exchange(getShardStatusPath, HttpMethod.GET, entity, ShardStatus.class, clusterId, shardId);
		return response.getBody();
	}

	
	@Override
	public void clusterAdded(ClusterMeta clusterMeta, ForwardInfo forwardInfo) {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, ForwardType.MULTICASTING);
		logger.info("[clusterAdded][forward]{},{}--> {}", clusterMeta.getId(), forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(clusterMeta, headers);
		restTemplate.exchange(changeClusterPath, HttpMethod.POST, entity, String.class, clusterMeta.getId());
	
	}

	@Override
	public void clusterModified(ClusterMeta clusterMeta, ForwardInfo forwardInfo) {

		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, ForwardType.MULTICASTING);
		logger.info("[clusterModified][forward]{},{} --> {}", clusterMeta.getId(), forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(clusterMeta, headers);
		restTemplate.exchange(changeClusterPath, HttpMethod.PUT, entity, String.class, clusterMeta.getId());
		
	}

	@Override
	public void clusterDeleted(String clusterId, ForwardInfo forwardInfo) {

		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo, ForwardType.MULTICASTING);
		logger.info("[clusterDeleted][forward]{},{} --> {}", clusterId, forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
		restTemplate.exchange(changeClusterPath, HttpMethod.DELETE, entity, String.class, clusterId);
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String ip, int port, ForwardInfo forwardInfo)
			throws Exception {
		
		HttpHeaders headers = checkCircularAndGetHttpHeaders(forwardInfo);
		logger.info("[updateUpstream][forward]{},{},{}:{}, {}--> {}", clusterId, shardId, ip, port, forwardInfo, this);
		
		HttpEntity<ClusterMeta> entity = new HttpEntity<>(headers);
		restTemplate.exchange(upstreamChangePath, HttpMethod.PUT, entity, String.class, clusterId, shardId, ip, port);
		
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
