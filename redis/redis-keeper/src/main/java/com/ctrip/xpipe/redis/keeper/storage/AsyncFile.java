package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;

public class AsyncFile {

    final String path;
    FileChannel channel;
    final boolean atomicReplace;
    final boolean isDirectory;

    AsyncFile(String path, FileChannel channel) {
        this(path, channel, false, false);
    }

    AsyncFile(String path, FileChannel channel, boolean atomicReplace) {
        this(path, channel, atomicReplace, false);
    }

    AsyncFile(String path, FileChannel channel, boolean atomicReplace, boolean isDirectory) {
        this.path = path;
        this.channel = channel;
        this.atomicReplace = atomicReplace;
        this.isDirectory = isDirectory;
    }
}
