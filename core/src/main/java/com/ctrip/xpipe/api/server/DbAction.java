package com.ctrip.xpipe.api.server;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:33:37
 */
public interface DbAction {
	
	
	void requestFullSync();
	
	void requestPartialSync();

}
