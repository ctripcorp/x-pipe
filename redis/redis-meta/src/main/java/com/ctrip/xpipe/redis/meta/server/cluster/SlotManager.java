package com.ctrip.xpipe.redis.meta.server.cluster;

import java.util.Map;
import java.util.Set;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface SlotManager extends Lifecycle{
	
	public static final int TOTAL_SLOTS = Integer.parseInt(System.getProperty("TOTAL_SLOTS", "1024"));//change only for unit test
	
	SlotInfo getSlotInfo(int slotId);
	
	Integer getSlotServerId(int slotId);
	
	Set<Integer> getSlotsByServerId(int serverId);

	int getSlotsSizeByServerId(int serverId);

	void refresh() throws Exception;
	
	void refresh(int ...slotIds) throws Exception;
	
	void move(int slotId, int fromServer, int toServer);
	
	Set<Integer>  allSlots();
	
	Set<Integer> allServers();
	
	Map<Integer, SlotInfo> allMoveingSlots();
	
	int getSlotIdByKey(Object key);

	SlotInfo getSlotInfoByKey(Object key);

	Integer getServerIdByKey(Object key);
	
	Map<Integer, SlotInfo> allSlotsInfo();

}
