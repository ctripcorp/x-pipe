package com.ctrip.xpipe.redis.core.store;

import java.io.Closeable;
import java.io.IOException;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 *         2016年4月19日 下午3:43:56
 */
public interface ReplicationStore extends Closeable {

	public static String BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME = "BACKUP_REDIS_MASTER";

	// rdb related
	RdbStore beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException;

	DumpedRdbStore prepareNewRdb() throws IOException;

	void rdbUpdated(DumpedRdbStore dumpedRdbStore) throws IOException;

	// command related
	int appendCommands(ByteBuf byteBuf) throws IOException;

	boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException;

	// full sync
	boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException;

	void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException;

	// meta related
	MetaStore getMetaStore();

	long getEndOffset();

	boolean isFresh();

	boolean checkOk();

	long getKeeperEndOffset();

	long nextNonOverlappingKeeperBeginOffset();

	// gc related
	void delete();

	boolean gc();
}
