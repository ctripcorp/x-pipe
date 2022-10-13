package com.ctrip.xpipe.redis.core.store;

import java.io.File;
import java.io.IOException;

public interface CommandReader<R> {

	R read() throws IOException;

	void flushed(R cmdContent);

	File getCurFile();

	void close() throws IOException;

}
