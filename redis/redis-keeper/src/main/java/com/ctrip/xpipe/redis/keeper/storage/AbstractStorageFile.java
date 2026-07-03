package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

abstract class AbstractStorageFile {

    long pendingFsyncBytes = 0;

    abstract FileChannel currentWriteChannel();

    abstract String identifier();
}
