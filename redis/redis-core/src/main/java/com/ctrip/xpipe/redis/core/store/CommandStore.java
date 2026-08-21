package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.lifecycle.Initializable;
import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface CommandStore extends Initializable, Closeable, Destroyable {

	int appendCommands(ByteBuf byteBuf) throws IOException;

	boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException;
	
	long totalLength();
	
	long lowestAvailableOffset();
	
	/**
	 * The lowest logical read offset among all CommandReader ({@link CommandReader#getReadOffset()}).
	 * Files / segments ending before this offset can be GCed.
	 */
	long lowestReadingOffset();

	void addCommandsListener(ReplicationProgress<?> replicationProgress, CommandsListener commandsListener) throws IOException;

	boolean retainCommands(CommandsGuarantee commandsGuarantee);

	List<BacklogOffsetReplicationProgress> locateCmdSegment(String uuid, long begGno, long endGno) throws IOException;

	long getCommandsLastUpdatedAt();
	
	void gc();

	void rotateFileIfNecessary() throws IOException;

	String simpleDesc();

	void addReader(CommandReader<?> reader);

	void removeReader(CommandReader<?> reader);

	void makeSureOpen();

	void attachRateLimiter(SyncRateLimiter rateLimiter);

	Pair<Long, GtidSet> locateContinueGtidSet(GtidSet gtidSet) throws IOException;

	Pair<Long, GtidSet> locateContinueGtidSetWithFallbackToEnd(GtidSet gtidSet) throws IOException;

	Pair<Long, GtidSet> locateTailOfCmd();

	GtidSet getIndexGtidSet();

	void switchToXSync(GtidSet gtidSet) throws IOException;

	void switchToPsync(String replId, long offset) throws IOException;

	/**
	 * Undo {@link #switchToPsync} for H2.B2 {@code switchToPSync} meta-commit rollback:
	 * restore {@code buildIndex=true} only. Must not {@code openWriter} — {@code closeWriter}
	 * does not unbind writers (true unbind is CP-R / CP3). Not used by {@code psyncContinue}
	 * (already PSYNC / Index write path idle).
	 */
	void restoreXsyncIndex() throws IOException;

	/**
	 * XSYNC reconnect ({@code xsyncContinue}): if Index is enabled and writers were unbound
	 * after rotate failure, rebind to the current tip. Already bound → no-op (T-H3.CP3).
	 */
	void rebindIndexWritersIfUnbound() throws IOException;

	int onlyAppendCommand(ByteBuf byteBuf) throws IOException;

	boolean increaseLostNotInCmdStore(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException ;

	void resetStateForContinue();

	void flushSlidingWindow() throws IOException;

	/**
	 * Flush pending cmd bytes (sliding window) and index writers if present.
	 * Explicit durability API — do not abuse {@link #totalLength()} / backlog helpers for this.
	 */
	void flushPendingData() throws IOException;
}
