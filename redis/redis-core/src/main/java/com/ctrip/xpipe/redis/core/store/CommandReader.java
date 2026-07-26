package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

public interface CommandReader<R> {

	R read() throws IOException;

	R read(long milliSeconds) throws IOException;

	/**
	 * Start offset of the segment currently being read
	 * (greatest known segment start {@code <=} reader position).
	 *
	 * @return segment start offset, or {@code -1} if unknown / no segment
	 */
	long getCurStartOffset();

	void flushed(R cmdContent);

	void close() throws IOException;
}
