package com.ctrip.xpipe.redis.core.store;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         2016年4月19日 下午3:43:56
 */
public interface ReplicationStore extends Closeable {
	
	public static String BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME = "BACKUP_REDIS_MASTER"; 

	RdbStore beginRdb(String masterRunid, long masterOffset, long rdbFileSize) throws IOException;

	CommandStore getCommandStore();
	
	MetaStore getMetaStore();
	
	/**
	 * delete all the data saved!
	 */
	void delete();
	
	void rdbUpdated(String rdbRelativePath, long masterOffset) throws IOException;
	
	/**
	 * the last byte offset
	 * 
	 * @return
	 */
	long getEndOffset();
	
	boolean gc();

	File prepareNewRdbFile();

	boolean fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException;

	boolean isFresh();

	long getKeeperEndOffset();

	long nextNonOverlappingKeeperBeginOffset();
}
