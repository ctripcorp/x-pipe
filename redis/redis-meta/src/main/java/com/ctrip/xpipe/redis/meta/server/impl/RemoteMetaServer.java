package com.ctrip.xpipe.redis.meta.server.impl;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.AbstractRemoteClusterServer;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class RemoteMetaServer extends AbstractRemoteClusterServer implements MetaServer{

	public RemoteMetaServer(int serverId) {
		super(serverId);
	}
	
	public RemoteMetaServer(int serverId, ClusterServerInfo clusterServerInfo) {
		super(serverId, clusterServerInfo);
	}

	@Override
	public KeeperMeta getActiveKeeper(String clusterId, String shardId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RedisMeta getRedisMaster(String clusterId, String shardId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeeperMeta getUpstreamKeeper(String clusterId, String shardId) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ShardStatus getShardStatus(String clusterId, String shardId) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateActiveKeeper(String clusterId, String shardId, KeeperMeta keeper) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateUpstream(String clusterId, String shardId, String upstream) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void promoteRedisMaster(String clusterId, String shardId, String promoteIp, int promotePort)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.meta.server.rest.AllMetaServerService#getKeepersByKeeperContainer(com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta)
	 */
	@Override
	public List<KeeperTransMeta> getKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService#addKeeper(java.lang.String, java.lang.String, com.ctrip.xpipe.redis.core.entity.KeeperMeta)
	 */
	@Override
	public void addKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService#removeKeeper(java.lang.String, java.lang.String, com.ctrip.xpipe.redis.core.entity.KeeperMeta)
	 */
	@Override
	public void removeKeeper(String clusterId, String shardId, KeeperMeta keeperMeta) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService#setKeepers(java.lang.String, java.lang.String, com.ctrip.xpipe.redis.core.entity.KeeperMeta, java.util.List)
	 */
	@Override
	public void setKeepers(String clusterId, String shardId, KeeperMeta keeperMeta, List<KeeperMeta> keeperMetas) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService#clusterChanged(java.lang.String)
	 */
	@Override
	public void clusterChanged(String clusterId) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService#getDynamicInfo()
	 */
	@Override
	public DcMeta getDynamicInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerService#getAllMetaServers()
	 */
	@Override
	public List<MetaServerMeta> getAllMetaServers() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService#ping(java.lang.String, java.lang.String, com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta)
	 */
	@Override
	public void ping(String clusterId, String shardId, KeeperInstanceMeta keeperInstanceMeta) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService#getAllKeepersByKeeperContainer(com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta)
	 */
	@Override
	public List<KeeperTransMeta> getAllKeepersByKeeperContainer(KeeperContainerMeta keeperContainerMeta) {
		// TODO Auto-generated method stub
		return null;
	}

}
