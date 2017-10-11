package com.ctrip.xpipe.redis.meta.server.cluster.impl;


import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.ObjectUtils.EqualFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public abstract class AbstractClusterServer extends AbstractLifecycleObservable implements ClusterServer{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	private int serverId;
	
	private ClusterServerInfo clusterServerInfo;

	public AbstractClusterServer() {
		
	}

	public AbstractClusterServer(int serverId, ClusterServerInfo clusterServerInfo) {
		
		this.serverId = serverId;
		this.clusterServerInfo = clusterServerInfo;
	}

	@Override
	public int hashCode() {
		return serverId;
	}
	
	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public void setClusterServerInfo(ClusterServerInfo clusterServerInfo) {
		this.clusterServerInfo = clusterServerInfo;
	}
	
	@Override
	public boolean equals(Object obj) {
		
		if(!(obj instanceof ClusterServer)){
			return false;
		}
		
		return ObjectUtils.equals(this, (ClusterServer)obj, new EqualFunction<ClusterServer>(){

			@Override
			public boolean equals(ClusterServer obj1, ClusterServer obj2) {
				return obj1.getServerId() == obj2.getServerId();
			}

		});
	}
	
	@Override
	public int getServerId() {
		return this.serverId;
	}

	@Override
	public ClusterServerInfo getClusterInfo() {
		return this.clusterServerInfo;
	}
	
	@Override
	public String toString() {
		return String.format("serverId:%d, (%s)", serverId, clusterServerInfo);
	}
	
}
