package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public interface Psync extends Command<Object>{
	
	public static final String FULL_SYNC = "FULLRESYNC";
	public static final String PARTIAL_SYNC = "CONTINUE";
	
	void addPsyncObserver(PsyncObserver observer);
	
	
	public static enum PSYNC_STATE{
		PSYNC_COMMAND_WAITING_REPONSE,
		READING_RDB,
		READING_COMMANDS
	}
	

}
