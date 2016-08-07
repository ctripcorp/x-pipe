package com.ctrip.xpipe.redis.meta.server.meta;

import java.util.Set;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.meta.DcMetaManager;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public interface CurrentMetaServerMetaManager extends Observable{
	
	Set<String> allClusters();
	
	void deleteSlot(int slotId);
	
	void addSlot(int slotId);

	void exportSlot(int slotId);

	void importSlot(int slotId);
	
	DcMetaManager getCurrentMeta();

}
