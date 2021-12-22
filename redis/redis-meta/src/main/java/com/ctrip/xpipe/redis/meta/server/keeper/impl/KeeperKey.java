package com.ctrip.xpipe.redis.meta.server.keeper.impl;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class KeeperKey {
	
	private Long clusterDbId;
	private Long shardDbId;
	private String ip;
	private int    port;
	
	public KeeperKey(Long clusterDbId, Long shardDbId, String ip, int port){
		this.clusterDbId = clusterDbId;
		this.shardDbId = shardDbId;
		this.ip = ip;
		this.port = port;
	}

	public Long getClusterDbId() {
		return clusterDbId;
	}

	public Long getShardDbId() {
		return shardDbId;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int hashCode() {
		return ObjectUtils.hashCode(clusterDbId, shardDbId, ip, port);
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(!(obj instanceof KeeperKey)){
			return false;
		}
		KeeperKey other = (KeeperKey) obj;
		if(!ObjectUtils.equals(clusterDbId, other.clusterDbId)){
			return false;
		}

		if(!ObjectUtils.equals(shardDbId, other.shardDbId)){
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
