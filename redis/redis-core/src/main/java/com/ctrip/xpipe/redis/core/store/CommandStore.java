package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.gtid.GtidSet;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface CommandStore<P extends ReplicationProgress<?,?>, R> extends Closeable, Destroyable{

	int appendCommands(ByteBuf byteBuf) throws IOException;

	CommandReader<R> beginRead(P replicationProgress) throws IOException;

	boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException;
	
	long totalLength();
	
	long lowestAvailableOffset();
	
	/**
	 * The lowest offset(start from zero) among all CommandReader.
	 * Files with lower offsets can be GCed.
	 */
	long lowestReadingOffset();

	void addCommandsListener(P replicationProgress, CommandsListener commandsListener) throws IOException;

	boolean retainCommands(CommandsGuarantee commandsGuarantee);

	long getCommandsLastUpdatedAt();
	
	void gc();

	void rotateFileIfNecessary() throws IOException;

	CommandFile newCommandFile(long startOffset) throws IOException;

	File findIndexFile(CommandFile commandFile);

	void addIndex(CommandFileOffsetGtidIndex index);

	CommandFile findFileForOffset(long offset) throws IOException;

	CommandFile findLatestFile() throws IOException;

	CommandFileSegment findFirstFileSegment(GtidSet excludedGtidSet) throws IOException;

	CommandFileSegment findLastFileSegment() throws IOException;

	GtidSet getBeginGtidSet() throws IOException;

	GtidSet getEndGtidSet();

	String simpleDesc();

	void addReader(CommandReader<R> reader);

	void removeReader(CommandReader<R> reader);

	CommandFile findNextFile(File file);

	void makeSureOpen();
	
}
