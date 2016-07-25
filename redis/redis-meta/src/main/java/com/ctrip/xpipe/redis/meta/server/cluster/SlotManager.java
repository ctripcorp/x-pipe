package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface SlotManager{
	
	public static final int TOTAL_SLOTS = 1 << 10;
	
	Integer getSlotServerId(int slotId);
	
	Set<Integer> getByServerId(int serverId);
	
	void refresh() throws Exception;
	
	void move(int slotId, int fromServer, int toServer);
	
	Set<Integer>  allSlots();
	
	Set<Integer> allServers();

}
