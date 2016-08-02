package com.ctrip.xpipe.redis.core.store;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public interface RdbStore {
	
	public enum Status {
		Writing, Success, Fail
	};

	int writeRdb(ByteBuf buf) throws IOException;

	void endRdb() throws IOException;
	
	void readRdbFile(final RdbFileListener rdbFileListener) throws IOException;
	
	int refCount();

	boolean delete();
	
	long lastKeeperOffset();

}
