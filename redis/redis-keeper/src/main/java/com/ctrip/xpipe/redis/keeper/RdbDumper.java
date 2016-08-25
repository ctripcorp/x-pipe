package com.ctrip.xpipe.redis.keeper;


import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public interface RdbDumper extends Command<Void>{

	void tryFullSync(RedisSlave redisSlave) throws IOException;

	File prepareRdbFile();

	void prepareDump();
	
	void beginReceiveRdbData(long masterOffset);
	
	void dumpFinished();

	void dumpFail(Throwable th);
	
	void exception(Throwable th);
	

}
