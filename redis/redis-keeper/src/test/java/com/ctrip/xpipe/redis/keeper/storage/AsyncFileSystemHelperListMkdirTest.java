package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncFileSystemHelperListMkdirTest extends AbstractTest {

    @Test
    public void awaitListWaitsLongerThanDefaultIoTimeout() throws Exception {
        CompletableFuture<List<String>> slowList = new CompletableFuture<>();
        long completeAfter = AsyncFileSystemHelper.DEFAULT_IO_TIMEOUT_MILLIS + 500;
        scheduled.schedule(() -> slowList.complete(Collections.singletonList("a")), completeAfter,
                TimeUnit.MILLISECONDS);

        List<String> result = AsyncFileSystemHelper.awaitList(() -> slowList, "slow list");
        Assert.assertEquals(Collections.singletonList("a"), result);
    }

    @Test
    public void awaitListTimesOutAfterListTimeout() {
        CompletableFuture<List<String>> neverList = new CompletableFuture<>();
        long start = System.currentTimeMillis();
        try {
            AsyncFileSystemHelper.awaitList(() -> neverList, "never list");
            Assert.fail("expected timeout");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("timeout"));
        }
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue("elapsed " + elapsed, elapsed >= AsyncFileSystemHelper.LIST_IO_TIMEOUT_MILLIS);
    }

    @Test
    public void plainAwaitKeepsDefaultTimeout() {
        CompletableFuture<List<String>> neverList = new CompletableFuture<>();
        long start = System.currentTimeMillis();
        try {
            AsyncFileSystemHelper.await(() -> neverList, "never");
            Assert.fail("expected timeout");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("timeout"));
        }
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue("elapsed " + elapsed, elapsed < AsyncFileSystemHelper.LIST_IO_TIMEOUT_MILLIS);
    }

    @Test
    public void mkdirRetriesOnceAfterTimeout() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        when(fs.mkdir("/d", true))
                .thenReturn(new CompletableFuture<>())
                .thenReturn(CompletableFuture.completedFuture(true));

        AtomicBoolean created = new AtomicBoolean();
        AsyncFileSystemHelper.runWithIoTimeout(50,
                () -> created.set(AsyncFileSystemHelper.mkdir(fs, "/d", true, "mkdir /d")));

        Assert.assertTrue(created.get());
        verify(fs, times(2)).mkdir("/d", true);
    }

    @Test
    public void mkdirRetriesOnceAfterFailure() throws Exception {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        CompletableFuture<Boolean> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IOException("EIO"));
        when(fs.mkdir("/d", true))
                .thenReturn(failed)
                .thenReturn(CompletableFuture.completedFuture(true));

        Assert.assertTrue(AsyncFileSystemHelper.mkdir(fs, "/d", true, "mkdir /d"));
        verify(fs, times(2)).mkdir("/d", true);
    }

    @Test
    public void mkdirThrowsWhenRetryAlsoFails() {
        AsyncFileSystem fs = mock(AsyncFileSystem.class);
        CompletableFuture<Boolean> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IOException("EIO"));
        when(fs.mkdir("/d", true)).thenReturn(failed);

        try {
            AsyncFileSystemHelper.mkdir(fs, "/d", true, "mkdir /d");
            Assert.fail("expected IOException");
        } catch (IOException e) {
            Assert.assertEquals("EIO", e.getMessage());
        }
        verify(fs, times(2)).mkdir("/d", true);
    }
}
