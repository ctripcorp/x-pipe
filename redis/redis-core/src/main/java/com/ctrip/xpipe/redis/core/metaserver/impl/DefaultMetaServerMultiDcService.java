package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcService;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.ProxyRedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.proxy.RedisProxy;

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
	
	public DefaultMetaServerMultiDcService(String metaServerAddress) {
		this(metaServerAddress, DEFAULT_RETRY_TIMES, DEFAULT_RETRY_INTERVAL_MILLI);
	}
	
	public DefaultMetaServerMultiDcService(String metaServerAddress, int retryTimes, int retryIntervalMilli) {
		super(retryTimes, retryIntervalMilli);
		this.metaServerAddress = metaServerAddress;
		upstreamchangePath = META_SERVER_SERVICE.UPSTREAM_CHANGE.getRealPath(metaServerAddress);
		upstreamPeerPath = META_SERVER_SERVICE.UPSTREAM_PEER_CHANGE.getRealPath(metaServerAddress);
		peerMasterPath = META_SERVER_SERVICE.GET_PEER_MASTER.getRealPath(metaServerAddress);
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
	public ProxyRedisMeta getPeerMaster(String clusterId, String shardId, RedisProxy proxy) {
		ProxyRedisMeta meta = ProxyRedisMeta.valueof(restTemplate.getForObject(peerMasterPath, RedisMeta.class, clusterId, shardId)).setProxy(proxy);
		return meta;
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
