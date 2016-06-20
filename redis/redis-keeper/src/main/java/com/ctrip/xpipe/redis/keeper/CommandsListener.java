package com.ctrip.xpipe.redis.keeper;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月19日 下午4:59:45
 */
public interface CommandsListener {

	
	void onCommand(ByteBuf byteBuf);
}
