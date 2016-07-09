package com.ctrip.xpipe.redis.meta.server.dao;


import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface MetaServerUpdateOperation {
	
	boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper) throws Exception;
	
	boolean updateRedisMaster(String clusterId, String shardId, RedisMeta redisMaster) throws Exception;
	
	boolean updateUpstreamKeeper(String clusterId, String shardId, String address) throws Exception;
	
}
