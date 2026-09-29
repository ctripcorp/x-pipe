package com.ctrip.xpipe.redis.keeper.health.job;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @see xpipe-copilot/changes/keeper-tfs-m3 spec §3.6 / T-DH.3
 */
public class DiskReadWriteCheckJobTest extends AbstractRedisKeeperTest {

    private static final String DISK_CHECK_DIR = "disk_check";

    @Test
    public void concurrentDifferentHostnamesWriteOwnFiles() throws Exception {
        String storePath = getTestFileDir();
        String hostA = "host-a";
        String hostB = "host-b";
        CyclicBarrier start = new CyclicBarrier(2);
        CountDownLatch done = new CountDownLatch(2);
        AtomicReference<Boolean> resultA = new AtomicReference<>();
        AtomicReference<Boolean> resultB = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        executors.execute(() -> {
            try {
                start.await(5, TimeUnit.SECONDS);
                resultA.set(new DiskReadWriteCheckJob(storePath, hostA).execute().get(5, TimeUnit.SECONDS));
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                done.countDown();
            }
        });
        executors.execute(() -> {
            try {
                start.await(5, TimeUnit.SECONDS);
                resultB.set(new DiskReadWriteCheckJob(storePath, hostB).execute().get(5, TimeUnit.SECONDS));
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                done.countDown();
            }
        });

        Assert.assertTrue(done.await(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw new AssertionError("concurrent disk check failed", error.get());
        }
        Assert.assertEquals(Boolean.TRUE, resultA.get());
        Assert.assertEquals(Boolean.TRUE, resultB.get());

        File fileA = probeFile(storePath, hostA);
        File fileB = probeFile(storePath, hostB);
        Assert.assertTrue(fileA.isFile());
        Assert.assertTrue(fileB.isFile());
        String contentA = readContent(fileA);
        String contentB = readContent(fileB);
        Assert.assertFalse(contentA.isEmpty());
        Assert.assertFalse(contentB.isEmpty());

        Assert.assertTrue(new DiskReadWriteCheckJob(storePath, hostA).execute().get(5, TimeUnit.SECONDS));
        Assert.assertEquals(contentB, readContent(fileB));

        Assert.assertFalse(new File(storePath, "foo").exists());
    }

    @Test
    public void localPathStillAvailable() throws Exception {
        String storePath = getTestFileDir();
        Assert.assertTrue(new DiskReadWriteCheckJob(storePath, "local-host").execute().get(5, TimeUnit.SECONDS));
        Assert.assertTrue(probeFile(storePath, "local-host").isFile());
        Assert.assertFalse(new File(storePath, "foo").exists());
    }

    private File probeFile(String storePath, String hostname) {
        return new File(new File(storePath, DISK_CHECK_DIR), hostname);
    }

    private String readContent(File file) throws IOException {
        return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8).trim();
    }
}
