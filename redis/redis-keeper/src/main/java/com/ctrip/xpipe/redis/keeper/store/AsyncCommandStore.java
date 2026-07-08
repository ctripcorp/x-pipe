package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;

import java.io.File;
import java.util.List;

public interface AsyncCommandStore {

    int DEFAULT_ASYNC_WRITE_MAX_BYTES = 65536;

    AsyncFileSystem getAsyncFileSystem();

    AsyncSegmentFile getAsyncSegmentFile();

    File getCommandBaseDir();

    String getCommandFileNamePrefix();

    List<String> getCommandIndexPrefixes();

    ReplId getFileSystemReplId();

    int getAsyncWriteMaxBytes();
}
