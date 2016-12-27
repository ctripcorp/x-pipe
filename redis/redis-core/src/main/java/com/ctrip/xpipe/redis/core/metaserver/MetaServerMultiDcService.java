package com.ctrip.xpipe.redis.core.metaserver;

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
	 * @param upstreamAddress
	 */
	void upstreamChange(String clusterId, String shardId, String ip, int port);
	

}
