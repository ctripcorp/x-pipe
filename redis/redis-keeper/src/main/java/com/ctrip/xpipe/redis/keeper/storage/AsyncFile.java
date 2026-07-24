package com.ctrip.xpipe.redis.keeper.storage;

import java.nio.channels.FileChannel;
import java.util.EnumSet;
import java.util.Set;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class AsyncFile extends AbstractStorageFile {

    final String path;
    FileChannel channel;
    final boolean canCloseByUser;

    AsyncFile(String path, boolean atomicReplace, OpenMode openMode) {
        this(path, atomicReplace, openMode, true);
    }

    AsyncFile(String path, boolean atomicReplace, OpenMode openMode, boolean canCloseByUser) {
        this(path, atomicReplace, openMode, canCloseByUser, StorageUtil.asyncFileKey(path));
    }

    AsyncFile(String path, boolean atomicReplace, OpenMode openMode, boolean canCloseByUser, String key) {
        super(openMode, atomicReplace, key);
        this.path = path;
        this.canCloseByUser = canCloseByUser;
    }

    @Override
    FileChannel currentWriteChannel() {
        return channel;
    }

    @Override
    void openCurrentChannel() throws java.io.IOException {
        if (openMode == OpenMode.READ) {
            channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ, StandardOpenOption.CREATE);
        } else {
            Set<? extends OpenOption> options = openMode == OpenMode.WRITE
                    ? EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE)
                    : EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            channel = FileChannel.open(Paths.get(path), options);
            if (!atomicReplace) {
                channel.position(channel.size());
            }
        }
    }

    @Override
    void reopenCurrentChannel() throws java.io.IOException {
        if (channel == null) {
            return;
        }
        channel.close();
        openCurrentChannel();
    }
}
