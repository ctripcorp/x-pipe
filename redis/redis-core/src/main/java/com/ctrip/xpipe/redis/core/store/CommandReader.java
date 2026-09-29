package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

public interface CommandReader<R> {

	R read() throws IOException;

	R read(long milliSeconds) throws IOException;

	/**
	 * Lowest logical offset already delivered by async {@code transferTo}
	 * (not the reader's scan / emit cursor).
	 * Used by CommandStore GC ({@code lowestReadingOffset}) as the read gate;
	 * must not lead the actual transferred offset, or GC may delete unread data.
	 * Not derived from AsyncSegmentFile.position because transferTo does not advance it.
	 */
	long getReadOffset();

	/**
	 * Start offset of the segment currently being read
	 * (greatest known segment start {@code <=} {@link #getReadOffset()}).
	 *
	 * @return segment start offset, or {@code -1} if unknown / no segment
	 */
	long getCurStartOffset();

	void flushed(R cmdContent);

	void close() throws IOException;
}
