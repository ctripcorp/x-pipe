package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
public class NoneCommandStoreDelay implements CommandStoreDelay{

	@Override
	public void beginWrite() {
		
	}

	@Override
	public void endWrite(long offset) {
		
	}

	@Override
	public void endRead(CommandsListener commandsListener, long offset) {

	}

	@Override
	public void beginSend(CommandsListener commandsListener, long offset) {
		
	}

	@Override
	public void flushSucceed(CommandsListener commandsListener, long offset) {
		
	}

}
