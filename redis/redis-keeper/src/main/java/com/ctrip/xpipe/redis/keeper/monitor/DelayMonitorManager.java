package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;

/**
 * @author wenchao.meng
 *
 * Nov 24, 2016
 */
public interface DelayMonitorManager {
	
	void beginWrite(CommandStore commandStore, long offset);
	
	void endWrite(CommandStore commandStore, long offset);

	void beginSend(CommandStore commandStore, CommandsListener commandsListener, long offset);
	
	void flushSucceed(CommandStore commandStore, CommandsListener commandsListener, long offset);

}
