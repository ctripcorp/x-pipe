package com.ctrip.xpipe.redis.meta.server.cluster.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Jul 26, 2016
 */
public abstract class AbstractSlotFailAction implements SlotMoveFailAction{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	
	@Override
	public void onFail(SlotMoveTask slotMoveTask) {
		
	}
	

	@Override
	public void onSuccess(SlotMoveTask slotMoveTask) {
		
	}
	
}
