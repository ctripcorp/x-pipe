package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public interface Psync extends Command<Object>{
	
	void addPsyncObserver(PsyncObserver observer);
}
