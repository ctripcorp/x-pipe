package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.api.command.Command;

import java.io.Closeable;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public interface Psync extends Command<Object>, Closeable {
	
	String FULL_SYNC = "FULLRESYNC";
	String PARTIAL_SYNC = "CONTINUE";
	long KEEPER_PARTIAL_SYNC_OFFSET = -2;
	long KEEPER_FRESH_RDB_SYNC_OFFSET = -3;
	
	void addPsyncObserver(PsyncObserver observer);
	
	
	enum PSYNC_STATE{
		PSYNC_COMMAND_WAITING_REPONSE,
		READING_RDB,
		READING_COMMANDS
	}
	

}
