package com.ctrip.xpipe.redis.core.store;

import io.netty.channel.ChannelFuture;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午4:59:45
 */
public interface CommandsListener {

	boolean isOpen();

	//CommandsListener.onCommand() support ByteBuf, FileRegion, RedisOp as input
	ChannelFuture onCommand(CommandFile currentFile, long filePosition, Object cmd);
	
	void beforeCommand();

	Long processedOffset();
}
