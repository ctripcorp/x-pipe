package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public interface RdbDumper extends Command<Void>{

	void tryFullSync(RedisSlave redisSlave) throws IOException;

	DumpedRdbStore prepareRdbStore() throws IOException;

	void beginReceiveRdbData(String replId, long masterOffset);

	void rdbGtidSetParsed();
	
	void dumpFinished();

	void dumpFail(Throwable th);
	
	void exception(Throwable th);

}
