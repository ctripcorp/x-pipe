package com.ctrip.xpipe.redis.meta.server.dcchange.exception;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerRuntimeException;

import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
public class AddSentinelException extends MetaServerRuntimeException{

	private static final long serialVersionUID = 1L;

	public AddSentinelException(InetSocketAddress sentinel, String clusterId, String shardId, String masterIp, int masterPort, Throwable th) {
		super(String.format("add cluster:%s,shard:%s, master:%s:%d to sentinel:%s fail", clusterId, shardId, masterIp, masterPort, sentinel), 
				th);
	}
	
	

}
