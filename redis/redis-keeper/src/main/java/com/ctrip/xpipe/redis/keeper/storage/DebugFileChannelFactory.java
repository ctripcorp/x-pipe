package com.ctrip.xpipe.redis.keeper.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;

import com.ctrip.xpipe.redis.keeper.storage.FileFaultInjector.Category;

/**
 * Opens {@link DebugFileChannel}s for a debug-mode keeper (keeper-tfs-m6 §17.3). The open itself is
 * injected as {@link Category#OPEN}; when it fails no file is opened. The injector's lifecycle
 * (start / close) belongs to whoever creates the factory.
 */
public class DebugFileChannelFactory implements FileChannelFactory {

    private final FileFaultInjector injector;

    public DebugFileChannelFactory(FileFaultInjector injector) {
        this.injector = Objects.requireNonNull(injector, "injector");
    }

    public FileFaultInjector getInjector() {
        return injector;
    }

    @Override
    public FileChannel open(Path path, Set<? extends OpenOption> options) throws IOException {
        String pathString = path.toString();
        injector.inject(Category.OPEN, pathString);
        return new DebugFileChannel(FileChannelFactory.DEFAULT.open(path, options), pathString, injector);
    }
}
