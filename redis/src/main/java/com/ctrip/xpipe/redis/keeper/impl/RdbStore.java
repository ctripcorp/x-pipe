package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public interface RdbStore {
	
	public enum Status {
		Writing, Success, Fail
	};

	int write(ByteBuffer buf) throws IOException;

	void endWrite() throws IOException;
	
	Status getStatus();

	RandomAccessFile getRdbFile() throws IOException;

}
