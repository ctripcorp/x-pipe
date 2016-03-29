package com.ctrip.xpipe.server;

import com.ctrip.xpipe.api.server.DbAction;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:34:20
 */
public abstract class AbstractDbAction implements DbAction{

	@Override
	public void requestFullSync() {
		
		doRequestFullSync();
	}

	protected abstract void doRequestFullSync();

	@Override
	public void requestPartialSync() {
		
		doRequestPartialSync();
		
	}

	protected abstract void doRequestPartialSync();

}
