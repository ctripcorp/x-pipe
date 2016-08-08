package com.ctrip.xpipe.redis.meta.server.keeper.impl;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class KeeperKey {
	
	private String clusterId;
	private String shardId;
	private String ip;
	private int    port;
	
	public KeeperKey(String clusterId, String shardId, String ip, int port){
		this.clusterId = clusterId;
		this.shardId = shardId;
		this.ip = ip;
		this.port = port;
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int hashCode() {
		return ObjectUtils.hashCode(clusterId, shardId, ip, port);
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(!(obj instanceof KeeperKey)){
			return false;
		}
		KeeperKey other = (KeeperKey) obj;
		if(!ObjectUtils.equals(clusterId, other.clusterId)){
			return false;
		}

		if(!ObjectUtils.equals(shardId, other.shardId)){
			return false;
		}

		if(!ObjectUtils.equals(ip, other.ip)){
			return false;
		}

		if(!ObjectUtils.equals(port, other.port)){
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		return Codec.DEFAULT.encode(this);
	}

}
