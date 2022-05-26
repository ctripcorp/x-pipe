package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import io.netty.channel.ChannelFuture;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午4:59:45
 */
public interface CommandsListener {

	boolean isOpen();
	
	ChannelFuture onCommand(ReferenceFileRegion referenceFileRegion);

	ChannelFuture onCommand(RedisOp redisOp);
	
	void beforeCommand();

	Long processedOffset();
}
