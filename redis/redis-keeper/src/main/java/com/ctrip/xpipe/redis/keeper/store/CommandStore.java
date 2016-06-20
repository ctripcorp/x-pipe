package com.ctrip.xpipe.redis.keeper.store;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public interface CommandStore {

	int appendCommands(ByteBuf byteBuf) throws IOException;

	CommandReader beginRead(long startOffset) throws IOException;

	boolean await(long offset, int timeMilli) throws InterruptedException;

	void await(long offset) throws InterruptedException;

	void close();
	
	
	long totalLength();
}
