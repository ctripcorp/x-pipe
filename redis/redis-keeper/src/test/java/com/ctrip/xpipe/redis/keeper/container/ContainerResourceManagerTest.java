package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleCloser;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.*;

/**
 * keeper-tfs-m6 P2 (spec §2.4): the container FS comes with a registered DeferredHandleCloser.
 */
public class ContainerResourceManagerTest {

    @Test
    public void testFsIsCreatedWithCloserAndShutdownStopsBoth() {
        ContainerResourceManager manager = new ContainerResourceManager();
        ReflectionTestUtils.setField(manager, "keeperConfig", new TestKeeperConfig());

        AsyncFileSystem fs = manager.getAsyncFileSystem();
        assertSame("one FS per container", fs, manager.getAsyncFileSystem());
        DeferredHandleCloser closer = DeferredHandleCloser.of(fs);
        assertNotNull("closer registered with the container FS", closer);
        assertSame("exposed for the unclosed-handles API", closer, manager.getDeferredHandleCloser());
        assertNotNull("metric reporter started with the closer",
                ReflectionTestUtils.getField(manager, "unclosedHandleMetricReporter"));
        assertEquals(0, closer.count());

        manager.shutdownAsyncFileSystem();
        assertNull("closer unregistered on shutdown", DeferredHandleCloser.of(fs));
        assertNull(manager.getDeferredHandleCloser());
        assertNull("metric reporter stopped", ReflectionTestUtils.getField(manager, "unclosedHandleMetricReporter"));
        assertNotSame("FS dropped and recreated on demand", fs, manager.getAsyncFileSystem());
        manager.shutdownAsyncFileSystem();
    }

    @Test
    public void testFlushRetryWindowConfig() {
        assertEquals(300_000L, KeeperConfig.DEFAULT_UNCLOSED_HANDLE_FLUSH_RETRY_WINDOW_MILLI);
        assertEquals("keeper.unclosed.handle.flush.retry.window.milli",
                KeeperConfig.KEY_UNCLOSED_HANDLE_FLUSH_RETRY_WINDOW_MILLI);
        TestKeeperConfig config = new TestKeeperConfig();
        assertEquals(300_000L, config.getUnclosedHandleFlushRetryWindowMilli());
        assertEquals(1_000L, config.setUnclosedHandleFlushRetryWindowMilli(1_000L)
                .getUnclosedHandleFlushRetryWindowMilli());
    }
}
