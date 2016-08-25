package com.ctrip.xpipe.redis.core.protocal;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:51:09
 */
public interface PsyncObserver {
	
	
	/**
	 * get FULLSYNC response
	 */
	void onFullSync();

	void reFullSync();

	/**
	 * get rdb length
	 * @param fileSize
	 * @param offset
	 * @throws IOException
	 */
	void beginWriteRdb(long fileSize, long masterRdbOffset) throws IOException;


	void endWriteRdb();
	
	void onContinue();
}
