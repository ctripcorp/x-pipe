package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         2016年4月19日 下午3:43:56
 */
public interface ReplicationStore extends Closeable, Destroyable {

	public static String BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME = "BACKUP_REDIS_MASTER";

	// rdb related
	RdbStore beginRdb(String replId, long rdbOffset, EofType eofType) throws IOException;

	void continueFromOffset(String replId, long continueOffset) throws IOException;
	
	DumpedRdbStore prepareNewRdb() throws IOException;

	void checkReplIdAndUpdateRdb(DumpedRdbStore dumpedRdbStore, String expectedReplId) throws IOException;

	// command related
	int appendCommands(ByteBuf byteBuf) throws IOException;

	boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException;

	// full sync
	boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException;

	void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException;

	void addCommandsListener(GtidSet excludedGtidSet, CommandsListener commandsListener) throws IOException;

	// meta related
	MetaStore getMetaStore();

	void shiftReplicationId(String newReplId) throws IOException;
	
	long getEndOffset();
	
	long firstAvailableOffset();

	GtidSet getBeginGtidSet() throws IOException;

	GtidSet getEndGtidSet();

	long beginOffsetWhenCreated();

	long lastReplDataUpdatedAt();

	boolean isFresh();

	boolean checkOk();

	boolean gc() throws IOException;
}
