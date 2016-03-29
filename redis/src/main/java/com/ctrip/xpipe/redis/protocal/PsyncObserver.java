package com.ctrip.xpipe.redis.protocal;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:51:09
 */
public interface PsyncObserver {

	void beginWriteRdb();
	
	void endWriteRdb();
	
	void setFullSyncInfo(String masterRunId, long masterOffset);
	
	void increaseReploffset();
	
	void increaseReploffset(int n);

}
