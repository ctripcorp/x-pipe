package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;

public interface CommandReader {

	void close() throws IOException;

	ReferenceFileRegion read() throws IOException;

}
