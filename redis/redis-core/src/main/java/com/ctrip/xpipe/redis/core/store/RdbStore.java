package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface RdbStore extends Destroyable, Closeable{
	
	public enum Status {
		Writing, Success, Fail
	};

	public enum Type {
		UNKNOWN, NORMAL, RORDB
	}

	void updateRdbType(Type type);

	Type getRdbType();

	String getReplId();

	long getRdbOffset();

	EofType getEofType();

	String getGtidSet();

	File getRdbFile();

	boolean updateRdbGtidSet(String gtidSet);

	boolean isGtidSetInit();

	boolean supportGtidSet();

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

	String getRdbFileName();

	boolean isWriting();

	long getRdbFileLastModified();

}
