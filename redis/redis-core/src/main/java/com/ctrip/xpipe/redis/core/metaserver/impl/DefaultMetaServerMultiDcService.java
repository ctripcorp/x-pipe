package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * Nov 3, 2016
 */
public class DefaultMetaServerMultiDcService extends AbstractMetaService implements MetaServerMultiDcService{

	private String  upstreamchangePath;
	private String  upstreamPeerPath;
	private String  metaServerAddress;
	private String peerMasterPath;
	private String sidsPath;
	
	public DefaultMetaServerMultiDcService(String metaServerAddress) {
		this(metaServerAddress, DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI);
	}
	
	public DefaultMetaServerMultiDcService(String metaServerAddress, int retryTimes, int retryIntervalMilli) {
		super(retryTimes, retryIntervalMilli);
		this.metaServerAddress = metaServerAddress;
		upstreamchangePath = META_SERVER_SERVICE.UPSTREAM_CHANGE.getRealPath(metaServerAddress);
		upstreamPeerPath = META_SERVER_SERVICE.UPSTREAM_PEER_CHANGE.getRealPath(metaServerAddress);
		peerMasterPath = META_SERVER_SERVICE.GET_PEER_MASTER.getRealPath(metaServerAddress);
		sidsPath = META_SERVER_SERVICE.GET_SIDS.getRealPath(metaServerAddress);
	}

	@Override
	public void upstreamChange(String clusterId, String shardId, String ip, int port) {
		
		restTemplate.put(upstreamchangePath, null, clusterId, shardId, ip, port);
	}

	@Override
	public void upstreamPeerChange(String dcId, String clusterId, String shardId) {
		restTemplate.put(upstreamPeerPath, null, dcId, clusterId, shardId);
	}

	@Override
	public RedisMeta getPeerMaster(String clusterId, String shardId) {
		return restTemplate.getForObject(peerMasterPath, RedisMeta.class, clusterId, shardId);
	}

	@Override
	public String getSids(String clusterId, String shardId) {
	    return restTemplate.getForObject(sidsPath, String.class, clusterId, shardId);
	}

	@Override
	protected List<String> getMetaServerList() {
		
		List<String> result = new ArrayList<>();
		result.add(metaServerAddress);
		return result;
	}
	
	@Override
	public String toString() {
		
		return String.format("%s[%s]", getClass().getSimpleName(), metaServerAddress);
	}
	
}
