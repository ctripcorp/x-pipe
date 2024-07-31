package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface CommandStore extends Initializable, Closeable, Destroyable {

	int appendCommands(ByteBuf byteBuf) throws IOException;

	boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException;
	
	long totalLength();
	
	long lowestAvailableOffset();
	
	/**
	 * The lowest offset(start from zero) among all CommandReader.
	 * Files with lower offsets can be GCed.
	 */
	long lowestReadingOffset();

	void addCommandsListener(ReplicationProgress<?> replicationProgress, CommandsListener commandsListener) throws IOException;

	boolean retainCommands(CommandsGuarantee commandsGuarantee);

	long getCommandsLastUpdatedAt();
	
	void gc();

	void rotateFileIfNecessary() throws IOException;

	CommandFile newCommandFile(long startOffset) throws IOException;

	File findIndexFile(CommandFile commandFile);

	void addIndex(CommandFileOffsetGtidIndex index);

	CommandFile findFileForOffset(long offset) throws IOException;

	CommandFile findLatestFile() throws IOException;

	CommandFileSegment findFirstFileSegment(GtidSet excludedGtidSet);

	CommandFileSegment findLastFileSegment();

	GtidSet getBeginGtidSet() throws IOException;

	String simpleDesc();

	void addReader(CommandReader<?> reader);

	void removeReader(CommandReader<?> reader);

	CommandFile findNextFile(File file);

	void makeSureOpen();

	default void setBaseIndex(String baseGtidSet, long localOffset) {
		//ignore
	}

	void attachRateLimiter(SyncRateLimiter rateLimiter);
}
