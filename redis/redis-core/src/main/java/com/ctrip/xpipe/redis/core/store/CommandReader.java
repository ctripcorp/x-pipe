package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;

import java.io.IOException;

public interface CommandReader {

	void close() throws IOException;

	ReferenceFileRegion read() throws IOException;

}
