package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;

// Opens every FileChannel of AsyncTFSBasedFileSystem and its file objects, so a decorated
// channel can be swapped in at FS creation (keeper-tfs-m6 §17).
@FunctionalInterface
public interface FileChannelFactory {

    FileChannelFactory DEFAULT = (path, options) -> FileChannel.open(path, options);

    FileChannel open(Path path, Set<? extends OpenOption> options) throws IOException;

    /** Called once by the owning FS on shutdown; releases resources the factory holds. */
    default void close() {
    }
}
