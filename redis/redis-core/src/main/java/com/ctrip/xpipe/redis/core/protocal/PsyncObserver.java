package com.ctrip.xpipe.redis.core.protocal;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:51:09
 */
public interface PsyncObserver {
	
	void reFullSync();
	
	void beginWriteRdb() throws IOException;
	
	void endWriteRdb();
	
	void onContinue();
}
