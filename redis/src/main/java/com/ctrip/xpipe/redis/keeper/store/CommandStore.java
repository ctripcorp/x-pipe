package com.ctrip.xpipe.redis.keeper.store;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public interface CommandStore {

	int appendCommands(ByteBuf byteBuf) throws IOException;

	CommandReader beginRead(long startOffset) throws IOException;

	void close();

}
