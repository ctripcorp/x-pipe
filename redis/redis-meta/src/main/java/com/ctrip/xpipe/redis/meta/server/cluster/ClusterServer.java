package com.ctrip.xpipe.redis.meta.server.cluster;

import com.ctrip.xpipe.api.command.CommandFuture;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface ClusterServer{
	
	int getServerId();
	
	ClusterServerInfo getClusterInfo();
	
	/**
	 * reresh slotmanager
	 */
	void notifySlotChange(int slot);
	
	/**
	 * notify server to export slot
	 * @param slotId
	 */
	CommandFuture<Void> exportSlot(int slotId);
	
	/**
	 * notify server to import slot
	 * @param slotId
	 */
	CommandFuture<Void> importSlot(int slotId);
	
}
