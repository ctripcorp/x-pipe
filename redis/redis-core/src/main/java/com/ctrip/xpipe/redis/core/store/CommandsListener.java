package com.ctrip.xpipe.redis.core.store;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午4:59:45
 */
public interface CommandsListener {

	boolean isOpen();
	
	void onCommand(ByteBuf byteBuf);
}
