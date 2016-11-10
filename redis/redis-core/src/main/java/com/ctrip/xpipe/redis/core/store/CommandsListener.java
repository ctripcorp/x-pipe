package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午4:59:45
 */
public interface CommandsListener {

	boolean isOpen();
	
	void onCommand(ReferenceFileRegion referenceFileRegion);
	
	void beforeCommand();
}
