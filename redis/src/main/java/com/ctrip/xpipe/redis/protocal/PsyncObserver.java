package com.ctrip.xpipe.redis.protocal;


/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:51:09
 */
public interface PsyncObserver {
	
	void reFullSync();
	
	void beginWriteRdb();
	
	void endWriteRdb();
	
	void onContinue();
}
