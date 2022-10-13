package com.ctrip.xpipe.redis.core.store;

/**
 * @author wenchao.meng
 *
 * Dec 25, 2016
 */
public interface RdbStoreListener {

	void onRdbGtidSet(String gtidSet);
	
	void onEndRdb();

}
