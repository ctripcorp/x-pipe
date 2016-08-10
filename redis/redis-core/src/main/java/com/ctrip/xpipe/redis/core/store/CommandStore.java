package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public interface CommandStore {

	int appendCommands(ByteBuf byteBuf) throws IOException;

	CommandReader beginRead(long startOffset) throws IOException;

	boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException;

	void close();
	
	
	long totalLength();
	
	/**
	 * The lowest offset(start from zero) among all CommandReader.
	 * Files with lower offsets can be GCed.
	 */
	long lowestReadingOffset();
	
	void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException;
	
}
