package com.ctrip.xpipe.redis.keeper;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public interface KeeperRepl {
	
	long getBeginOffset();
	
	long getEndOffset();
	
	/**
	 * transfer keeper offset to replicationStoreOffset
	 * @param offset
	 * @param commandsListener
	 * @throws IOException
	 */
	void addCommandsListener(long offset, CommandsListener commandsListener);
}
