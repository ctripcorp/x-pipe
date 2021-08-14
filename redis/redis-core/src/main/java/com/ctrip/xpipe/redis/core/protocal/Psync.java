package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public interface Psync extends Command<Object>{
	
	String FULL_SYNC = "FULLRESYNC";
	String PARTIAL_SYNC = "CONTINUE";
	long KEEPER_PARTIAL_SYNC_OFFSET = -2;
	
	void addPsyncObserver(PsyncObserver observer);
	
	
	enum PSYNC_STATE{
		PSYNC_COMMAND_WAITING_REPONSE,
		READING_RDB,
		READING_COMMANDS
	}
	

}
