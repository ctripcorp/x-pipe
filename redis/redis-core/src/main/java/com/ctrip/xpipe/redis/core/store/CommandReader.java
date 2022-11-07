package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

public interface CommandReader<R> {

	R read() throws IOException;

	CommandFile getCurCmdFile();

	//The filePosition is right after cmd is read
	long filePosition() throws IOException;

	void flushed(R cmdContent);

	void close() throws IOException;
}
