package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.storage.IndexFileMapping;

import java.io.File;
import java.util.List;

public interface AsyncCommandStore {

    int DEFAULT_ASYNC_WRITE_MAX_BYTES = 65536;

    AsyncFileSystem getAsyncFileSystem();

    AsyncSegmentFile getAsyncSegmentFile();

    File getCommandBaseDir();

    String getCommandFileNamePrefix();

    List<IndexFileMapping> getCommandIndexFileMappings();

    int getAsyncWriteMaxBytes();
}
