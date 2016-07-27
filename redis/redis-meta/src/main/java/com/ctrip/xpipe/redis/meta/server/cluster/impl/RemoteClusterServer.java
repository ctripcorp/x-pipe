package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public class RemoteClusterServer extends AbstractClusterServer{

	public RemoteClusterServer(int serverId, ClusterServerInfo clusterServerInfo) {
		super(serverId, clusterServerInfo);
	}

	@Override
	public void notifySlotChange() {
		
	}

	@Override
	public CommandFuture<Void> exportSlot(int slotId) {
		return null;
	}

	@Override
	public CommandFuture<Void> importSlot(int slotId) {
		return null;
	}



}
