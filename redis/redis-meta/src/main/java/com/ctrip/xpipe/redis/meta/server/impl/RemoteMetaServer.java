package com.ctrip.xpipe.redis.meta.server.impl;


import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
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

	public RemoteMetaServer(int currentServerId, int serverId) {
		super(currentServerId, serverId);
	}
	
	public RemoteMetaServer(int currentServerId, int serverId, ClusterServerInfo clusterServerInfo) {
		super(currentServerId, serverId, clusterServerInfo);
		
		pingPath = String.format("%s/%s", getHttpHost(), MetaServerKeeperService.PATH_PING);
	}

	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId) {
		throw new UnsupportedOperationException();
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
	public ShardStatus getShardStatus(String clusterId, String shardId) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateActiveKeeper(String clusterId, String shardId, KeeperMeta keeper) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String upstream) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void promoteRedisMaster(String clusterId, String shardId, String promoteIp, int promotePort)
			throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta, ForwardInfo forwardInfo) {
		checkCircular(forwardInfo);

		logger.info("[ping][forward]{},{},{}", clusterId, shardId, forwardInfo);
		if(forwardInfo == null){
			forwardInfo = new ForwardInfo(ForwardType.FORWARD);
		}
		forwardInfo.addForwardServers(getCurrentServerId());	
		
		HttpHeaders headers = new HttpHeaders();
		headers.add(MetaServerService.HTTP_HEADER_FOWRARD, Codec.DEFAULT.encode(forwardInfo));
		HttpEntity<KeeperInstanceMeta> entity = new HttpEntity<KeeperInstanceMeta>(keeperInstanceMeta, headers);
		restTemplate.postForObject(pingPath, entity, String.class, clusterId, shardId);
	}

	private void checkCircular(ForwardInfo forwardInfo) {
		if(forwardInfo != null && forwardInfo.hasServer(getCurrentServerId())){
			throw new CircularForwardException(forwardInfo, getCurrentServerId());
		}
	}
	
}
