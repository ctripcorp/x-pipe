package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ctrip.xpipe.redis.keeper.RdbFile;

public interface RdbStore {

	int write(ByteBuffer buf) throws IOException;

	void endWrite() throws IOException;

	RdbFile getRdbFile() throws IOException;

}
