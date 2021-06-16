package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;

/**
 * @author wenchao.meng
 *
 * Nov 3, 2016
 */
public interface MetaServerMultiDcService extends MetaServerService{


	/**
	 * used by backup dc
	 * @param clusterId
	 * @param shardId
	 * @param ip
	 * @param port
	 */
	void upstreamChange(String clusterId, String shardId, String ip, int port);

	void upstreamPeerChange(String dcId, String clusterId, String shardId);

	ProxyRedisMeta getPeerMaster(String clusterId, String shardId, RedisProxy proxy);

}
