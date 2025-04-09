package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *
 *         2016年4月19日 下午3:43:56
 */
public interface ReplicationStore extends Closeable, Destroyable {

	public static String BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME = "BACKUP_REDIS_MASTER";

	XSyncContinue locateContinueGtidSet(GtidSet gtidSet) throws Exception;

	void updateGtidSet(GtidSet gtidSet);

	void switchToPSync(String replId, long offset) throws IOException;

	void switchToXSync(GtidSet gtidSet, String masterUuid) throws IOException;

	/**
	 * @return pair of GtidSet.executed and GtidSet.lost
	 */
	Pair<GtidSet, GtidSet> getGtidSet();

	RdbStore prepareRdb(String replId, long rdbOffset, EofType eofType) throws IOException;

	// use in dumped, broken if replId dismatch with default repl
	void checkReplId(String expectReplId);

	void confirmRdb(RdbStore rdbStore) throws IOException;

	void continueFromOffset(String replId, long continueOffset) throws IOException;
	
	DumpedRdbStore prepareNewRdb() throws IOException;

	void checkReplIdAndUpdateRdb(RdbStore rdbStore) throws IOException;

	// command related
	int appendCommands(ByteBuf byteBuf) throws IOException;

	boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException;

	// full sync
	FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException;

	FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener, boolean masterSupportRordb) throws IOException;

	//create index
	FULLSYNC_FAIL_CAUSE createIndexIfPossible(ExecutorService indexingExecutors);

	void addCommandsListener(ReplicationProgress<?> progress, CommandsListener commandsListener) throws IOException;
	// meta related
	MetaStore getMetaStore();

	void shiftReplicationId(String newReplId) throws IOException;
	
	long getEndOffset();
	
	long firstAvailableOffset();

	long backlogBeginOffset();

	long backlogEndOffset();

	GtidSet getBeginGtidSet() throws IOException;

	GtidSet getEndGtidSet();

	boolean supportGtidSet();

	long beginOffsetWhenCreated();

	long lastReplDataUpdatedAt();

	boolean isFresh();

	boolean checkOk();

	void releaseRdb() throws IOException;

	boolean gc() throws IOException;
}
