package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class AsyncFile extends AbstractStorageFile {

    FileChannel channel;
    final boolean canCloseByUser;
    final boolean lenient;

    AsyncFile(String path, ReplaceMode replaceMode, OpenMode openMode, String key, String ioKey, boolean lenient) {
        this(path, replaceMode, openMode, true, key, ioKey, lenient, null);
    }

    AsyncFile(String path, ReplaceMode replaceMode, OpenMode openMode, String key, String ioKey, boolean lenient,
            String tenant) {
        this(path, replaceMode, openMode, true, key, ioKey, lenient, tenant);
    }

    AsyncFile(String path, ReplaceMode replaceMode, OpenMode openMode, boolean canCloseByUser,
            String key, String ioKey, boolean lenient) {
        this(path, replaceMode, openMode, canCloseByUser, key, ioKey, lenient, null);
    }

    AsyncFile(String path, ReplaceMode replaceMode, OpenMode openMode, boolean canCloseByUser,
            String key, String ioKey, boolean lenient, String tenant) {
        super(openMode, replaceMode, key, ioKey, path, Paths.get(path).getParent().toString(), tenant);
        this.canCloseByUser = canCloseByUser;
        this.lenient = lenient;
    }

    @Override
    FileChannel currentWriteChannel() {
        return channel;
    }

    @Override
    long openCurrentChannel() throws IOException {
        FileChannel oldChannel = null;
        FileChannel newChannel = null;
        try {
            long offset = -1;
            if (openMode == OpenMode.READ) {
                // Java cannot pair READ with CREATE; ATOMIC_PREFER_TMP may open before the target
                // exists (content still only in tmp). Leave channel null; IO retries open or
                // treats a still-missing target as empty.
                try {
                    newChannel = channelFactory.open(Paths.get(path), EnumSet.of(StandardOpenOption.READ));
                } catch (NoSuchFileException e) {
                    if (preferTmp()) {
                        return -1;
                    }
                    throw e;
                }
            } else {
                Set<? extends OpenOption> options = openMode == OpenMode.WRITE
                        ? EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE)
                        : EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                newChannel = channelFactory.open(Paths.get(path), options);
                offset = newChannel.size();
                newChannel.position(offset);
            }

            synchronized (this) {
                if (closed) {
                    // Closed while we were opening: the channel we just created must be released.
                    throw new IllegalStateException("file is closed: " + path);
                }
                oldChannel = channel;
                channel = newChannel;
                newChannel = null;
            }
            return offset;
        } finally {
            StorageUtil.closeChannelQuietly(newChannel);
            StorageUtil.closeChannelQuietly(oldChannel);
        }
    }

    @Override
    List<FileChannel> detachCurrentChannels() {
        if (channel == null) {
            return Collections.emptyList();
        }
        FileChannel detached = channel;
        channel = null;
        return Collections.singletonList(detached);
    }
}
