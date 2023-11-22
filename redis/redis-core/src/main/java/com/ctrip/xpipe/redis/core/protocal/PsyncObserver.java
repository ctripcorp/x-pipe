package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:51:09
 */
public interface PsyncObserver {
	
	/**
	 * get FULLSYNC response
	 * @param masterRdbOffset
	 */
	void onFullSync(long masterRdbOffset);

	void reFullSync();

	/**
	 * get rdb length
	 * @param fileSize
	 * @param offset
	 * @throws IOException
	 */
	void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException;

	void readRdbGtidSet(RdbStore rdbStore, String gtidSet);

	void readAuxEnd(RdbStore rdbStore);

	void endWriteRdb();
	
	void onContinue(String requestReplId, String responseReplId);

	void onKeeperContinue(String replId, long beginOffset);

}
