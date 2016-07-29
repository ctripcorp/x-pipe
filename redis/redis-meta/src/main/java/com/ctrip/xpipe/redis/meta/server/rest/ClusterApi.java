package com.ctrip.xpipe.redis.meta.server.rest;


import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;

/**
 * @author wenchao.meng
 *
 * Jul 29, 2016
 */
public interface ClusterApi{

	
	int getServerId();
	
	ClusterServerInfo getClusterInfo();
	
	void notifySlotChange();
	
	void exportSlot(int slotId) throws Exception;
	
	void importSlot(int slotId) throws Exception;
	

}
