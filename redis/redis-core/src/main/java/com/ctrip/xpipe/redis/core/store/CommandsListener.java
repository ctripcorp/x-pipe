package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import io.netty.channel.ChannelFuture;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午4:59:45
 */
public interface CommandsListener {

	boolean isOpen();
	
	ChannelFuture onCommand(ReferenceFileRegion referenceFileRegion);
	
	void beforeCommand();

	Long processedOffset();
}
