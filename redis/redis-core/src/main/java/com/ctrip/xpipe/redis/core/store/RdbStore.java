package com.ctrip.xpipe.redis.core.store;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.ctrip.xpipe.api.lifecycle.Destroyable;

import io.netty.buffer.ByteBuf;

public interface RdbStore extends Destroyable, Closeable{
	
	public enum Status {
		Writing, Success, Fail
	};

	int writeRdb(ByteBuf buf) throws IOException;

	void truncateEndRdb(int reduceLen) throws IOException;
	
	void endRdb() throws IOException;
	
	void failRdb(Throwable th);
	
	void readRdbFile(final RdbFileListener rdbFileListener) throws IOException;
	
	int refCount();

	long rdbOffset();
	
	long rdbFileLength();

	void incrementRefCount();

	void decrementRefCount();

	boolean checkOk();
	
	void addListener(RdbStoreListener rdbStoreListener);

	void removeListener(RdbStoreListener rdbStoreListener);
	
	boolean sameRdbFile(File file);

}
