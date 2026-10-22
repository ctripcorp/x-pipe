package com.ctrip.xpipe.redis.keeper.storage;

import org.junit.After;

/**
 * keeper-tfs-m6 §17.3: reruns {@link AsyncTFSBasedFileSystemTest} with every channel opened as a
 * {@link DebugFileChannel} and no fault configured, proving the wrapper is transparent.
 */
public class AsyncTFSBasedFileSystemDebugChannelTest extends AsyncTFSBasedFileSystemTest {

    private FileFaultInjector injector;

    @Override
    protected FileChannelFactory channelFactory() {
        injector = new FileFaultInjector(new MutableDebugFileChannelConfig());
        return new DebugFileChannelFactory(injector);
    }

    @After
    public void closeInjector() {
        injector.close();
    }
}
