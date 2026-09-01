package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncFile extends AbstractStorageFile {

    private static final Logger logger = LoggerFactory.getLogger(AsyncFile.class);

    FileChannel channel;
    final boolean canCloseByUser;

    AsyncFile(String path, boolean atomicReplace, OpenMode openMode, String key, String ioKey) {
        this(path, atomicReplace, openMode, true, key, ioKey);
    }

    AsyncFile(String path, boolean atomicReplace, OpenMode openMode, boolean canCloseByUser,
            String key, String ioKey) {
        super(openMode, atomicReplace, key, ioKey, path, Paths.get(path).getParent().toString());
        this.canCloseByUser = canCloseByUser;
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
                newChannel = FileChannel.open(Paths.get(path),
                        StandardOpenOption.READ, StandardOpenOption.CREATE);
            } else {
                Set<? extends OpenOption> options = openMode == OpenMode.WRITE
                        ? EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE)
                        : EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                newChannel = FileChannel.open(Paths.get(path), options);
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
            closeChannelQuietly(newChannel, "abandoned channel candidate");
            closeChannelQuietly(oldChannel, "replaced channel");
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

    private void closeChannelQuietly(FileChannel channel, String reason) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            logger.error("failed to close {} for {}", reason, path, e);
        }
    }
}
