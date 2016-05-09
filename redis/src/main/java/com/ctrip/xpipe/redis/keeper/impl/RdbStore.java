package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public interface RdbStore {

	int write(ByteBuffer buf) throws IOException;

	void endWrite() throws IOException;
	
	boolean isWriteDone();

	RandomAccessFile getRdbFile() throws IOException;

}
