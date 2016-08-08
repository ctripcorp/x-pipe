package com.ctrip.xpipe.redis.meta.server.rest;


import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;

/**
 * @author wenchao.meng
 *
 * Jul 29, 2016
 */
public interface ClusterApi{

	public static final String PATH_PREFIX = "/api/metacluster";

	public static final String PATH_NOTIFY_SLOT_CHANGE = "/notifyslotchange/{slotId}";
	public static final String PATH_EXPORT_SLOT = "/exportslot/{slotId}";
	public static final String PATH_IMPORT_SLOT = "/importslot/{slotId}";

	public static final String PATH_ADD_SLOT = "/addslot/{slotId}";
	public static final String PATH_DELETE_SLOT = "/deleteslot/{slotId}";

	int getServerId();
	
	ClusterServerInfo getClusterInfo();

	void addSlot(int slotId) throws Exception;
	
	void deleteSlot(int slotId) throws Exception;
	
	void exportSlot(int slotId) throws Exception;
	
	void importSlot(int slotId) throws Exception;

	void notifySlotChange(int slotId) throws Exception;

	String debug();
	
	void refresh() throws Exception;

}
