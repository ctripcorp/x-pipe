package com.ctrip.xpipe.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Jun 2, 2016
 */
public interface ByteBufReadAction {
	
	void read(Channel channel, ByteBuf byteBuf) throws ByteBufReadActionException;

}
