package com.ctrip.xpipe.redis.console.web.service;

import java.util.Set;

import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author shyin
 *
 * Aug 3, 2016
 */
public class ConsoleServiceImpl implements ConsoleService {

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#getAllDcIds()
	 */
	@Override
	public Set<String> getAllDcIds() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#getAllClusterIds()
	 */
	@Override
	public Set<String> getAllClusterIds() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#getClusterShardIds(java.lang.String)
	 */
	@Override
	public Set<String> getClusterShardIds(String clusterId) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#getDcMeta(java.lang.String)
	 */
	@Override
	public DcMeta getDcMeta(String dcId) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#getClusterMeta(java.lang.String, java.lang.String)
	 */
	@Override
	public ClusterMeta getClusterMeta(String dcId, String clusterId) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#getShardMeta(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ShardMeta getShardMeta(String dcId, String clusterId, String shardId) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#keeperActiveChanged(java.lang.String, java.lang.String, java.lang.String, com.ctrip.xpipe.redis.core.entity.KeeperMeta)
	 */
	@Override
	public void keeperActiveChanged(String dc, String clusterId, String shardId, KeeperMeta newActiveKeeper)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ctrip.xpipe.redis.core.console.ConsoleService#redisMasterChanged(java.lang.String, java.lang.String, java.lang.String, com.ctrip.xpipe.redis.core.entity.RedisMeta)
	 */
	@Override
	public void redisMasterChanged(String dc, String clusterId, String shardId, RedisMeta newRedisMaster)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

}
