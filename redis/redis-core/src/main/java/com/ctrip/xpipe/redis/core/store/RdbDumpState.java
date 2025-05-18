package com.ctrip.xpipe.redis.core.store;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public enum RdbDumpState {
	
	NORMAL,
	WAIT_DUMPPING,
	AUX_PARSED,
	DUMPING,
	FAIL,
	WAIT_RETRY,

}
