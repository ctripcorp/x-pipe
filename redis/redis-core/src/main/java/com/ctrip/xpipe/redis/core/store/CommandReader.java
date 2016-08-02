package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CommandReader {

	void close() throws IOException;

	int read(ByteBuffer dst) throws IOException;

}
