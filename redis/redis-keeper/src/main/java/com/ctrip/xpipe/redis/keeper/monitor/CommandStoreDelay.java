package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.core.store.CommandsListener;

/**
 * @author wenchao.meng
 *
 * Nov 25, 2016
 */
public interface CommandStoreDelay {
	
	void beginWrite();
	
	void endWrite(long offset);

	void endRead(final CommandsListener commandsListener, final long offset);

	void beginSend(CommandsListener commandsListener, long offset);

	void flushSucceed(CommandsListener commandsListener, long offset);


}
