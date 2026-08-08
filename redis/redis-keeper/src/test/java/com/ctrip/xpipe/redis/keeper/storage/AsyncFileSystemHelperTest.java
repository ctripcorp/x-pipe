package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Phase S / T-S.4: awaitOpen abandon closes late-arriving open handles.
 */
public class AsyncFileSystemHelperTest extends AbstractTest {

    @Test
    public void awaitOpenTimeoutAbandonsLateSuccessAndCloses() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile lateFile = mock(AsyncFile.class);
        CountDownLatch closeCalled = new CountDownLatch(1);
        when(fs.close(any(AsyncFile.class))).thenAnswer(invocation -> {
            closeCalled.countDown();
            return CompletableFuture.completedFuture(null);
        });

        CompletableFuture<AsyncFile> openFuture = new CompletableFuture<>();
        AtomicReference<IOException> thrown = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                        AsyncFileSystemHelper.awaitOpen(fs, openFuture, "slow open"));
                Assert.fail("expected timeout");
            } catch (IOException e) {
                thrown.set(e);
            }
        });
        waiter.start();
        waiter.join(TimeUnit.SECONDS.toMillis(5));
        Assert.assertFalse(waiter.isAlive());
        Assert.assertNotNull(thrown.get());
        Assert.assertTrue(thrown.get().getMessage().contains("timeout"));

        openFuture.complete(lateFile);
        Assert.assertTrue("late open should be closed", closeCalled.await(3, TimeUnit.SECONDS));
        verify(fs).close(lateFile);
    }

    @Test
    public void awaitOpenReturnsHandleWhenCompletedInTime() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        AsyncFile file = mock(AsyncFile.class);
        CompletableFuture<AsyncFile> openFuture = CompletableFuture.completedFuture(file);

        AsyncFile opened = AsyncFileSystemHelper.awaitOpen(fs, openFuture, "fast open");
        Assert.assertSame(file, opened);
    }

    @Test
    public void awaitTimeoutCancelsFuture() throws Exception {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AsyncFileSystemHelper.runWithIoTimeout(50, () ->
                    AsyncFileSystemHelper.await(future, "slow size"));
            Assert.fail("expected timeout");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("timeout"));
        }
        Assert.assertTrue(future.isCancelled());
    }
}
