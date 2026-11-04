package com.ctrip.xpipe.redis.keeper.container;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.store.ReplicationStoreDeleter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Container-level orphan-store GC fallback — real, file-backed {@link AsyncFileSystem} (same helper
 * {@link DefaultReplicationStoreManagerTest} uses) so the probe-open / mtime / delete-order behavior is
 * exercised against actual files, not hand-stubbed futures.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class KeeperContainerServiceGcTest extends AbstractRedisKeeperTest {

    @Mock
    private KeeperContainerConfig keeperContainerConfig;
    @Mock
    private ContainerResourceManager containerResourceManager;
    @Mock
    private RedisKeeperServer redisKeeperServer;

    private TestKeeperConfig keeperConfig;
    private KeeperContainerService keeperContainerService;
    private AsyncFileSystem fs;
    private File storeRoot;
    private ExecutorService gcExecutor;

    @Before
    public void setUp() {
        keeperConfig = new TestKeeperConfig();
        keeperConfig.setOrphanStoreMinIdleMilliToGc(TimeUnit.DAYS.toMillis(1));

        storeRoot = new File(getTestFileDir());
        storeRoot.mkdirs();
        fs = asyncFileSystem();

        keeperContainerService = new KeeperContainerService();
        ReflectionTestUtils.setField(keeperContainerService, "keeperContainerConfig", keeperContainerConfig);
        ReflectionTestUtils.setField(keeperContainerService, "keeperConfig", keeperConfig);
        ReflectionTestUtils.setField(keeperContainerService, "containerResourceManager", containerResourceManager);
        // doInitialize() isn't called in this test (no diskHealthChecker wired), so gcExecutor is set
        // directly, same as every other field above.
        gcExecutor = Executors.newFixedThreadPool(keeperConfig.getOrphanStoreGcParallelism());
        ReflectionTestUtils.setField(keeperContainerService, "gcExecutor", gcExecutor);

        when(keeperContainerConfig.getReplicationStoreDir()).thenReturn(storeRoot.getAbsolutePath());
        when(keeperContainerConfig.isTfsMode()).thenReturn(true);
        when(containerResourceManager.getAsyncFileSystem()).thenReturn(fs);
    }

    @After
    public void tearDown() {
        gcExecutor.shutdownNow();
        deleteRecursively(storeRoot);
    }

    private void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    private File replDir(long replId) {
        return new File(storeRoot, ReplId.from(replId).toString());
    }

    private void writeMeta(long replId, long ageMillis) throws Exception {
        File dir = replDir(replId);
        dir.mkdirs();
        File metaFile = new File(dir, ReplicationStoreDeleter.META_FILE);
        try (FileOutputStream out = new FileOutputStream(metaFile)) {
            out.write("latest.store.dir=x".getBytes());
        }
        assertTrue(metaFile.setLastModified(System.currentTimeMillis() - ageMillis));
        // TfsStoreDeleter checks the newest mtime of the directory and its children first.
        assertTrue(dir.setLastModified(System.currentTimeMillis() - ageMillis));
    }

    @Test
    public void testGcOrphanStoresBatchDelegatesToEachReplIdIndependently() throws Exception {
        writeMeta(9L, TimeUnit.DAYS.toMillis(2));   // old enough -> deleted
        writeMeta(10L, TimeUnit.MINUTES.toMillis(1)); // too fresh -> skipped
        // 11L: nothing on disk -> not found

        List<GcOrphanStoreResult> results = keeperContainerService.gcOrphanStores(Arrays.asList(9L, 10L, 11L));

        assertEquals(3, results.size());
        assertEquals(GcOrphanStoreResult.Status.DELETED, results.get(0).status);
        assertEquals(GcOrphanStoreResult.Status.SKIPPED, results.get(1).status);
        assertEquals(GcOrphanStoreResult.Status.NOT_FOUND, results.get(2).status);
        assertFalse(replDir(9L).exists());
        assertTrue(new File(replDir(10L), ReplicationStoreDeleter.META_FILE).exists());
    }

    @Test(expected = RedisKeeperRuntimeException.class)
    public void testListStoreDirNamesThrowsInBmMode() {
        when(keeperContainerConfig.isTfsMode()).thenReturn(false);
        keeperContainerService.listStoreDirNames();
    }

    @Test(expected = RedisKeeperRuntimeException.class)
    public void testListStoreDirNamesThrowsWhenRootMissing() {
        // an unlistable root is a failure, not an empty mount
        when(keeperContainerConfig.getReplicationStoreDir()).thenReturn(new File(storeRoot, "missing").getAbsolutePath());
        keeperContainerService.listStoreDirNames();
    }

    @Test
    public void testListStoreDirNamesReturnsAllEntries() throws Exception {
        writeMeta(1L, 0);
        writeMeta(2L, 0);
        assertTrue(new File(storeRoot, "stray.txt").createNewFile());

        List<String> names = keeperContainerService.listStoreDirNames();

        // no per-entry isDirectory: the metaserver keeps only repl_<id> names
        assertEquals(3, names.size());
        assertTrue(names.contains(ReplId.from(1L).toString()));
        assertTrue(names.contains(ReplId.from(2L).toString()));
        assertTrue(names.contains("stray.txt"));
    }

    @Test
    public void testListStoreDirNamesEmptyRootIsEmpty() {
        assertTrue(keeperContainerService.listStoreDirNames().isEmpty());
    }

    @Test
    public void testGcOrphanStoreNotFoundWhenNothingThere() {
        GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(42L);

        assertEquals(GcOrphanStoreResult.Status.NOT_FOUND, result.status);
        assertFalse(replDir(42L).exists());
    }

    @Test
    public void testGcOrphanStoreSkipsWhenMetaTooFresh() throws Exception {
        writeMeta(3L, TimeUnit.MINUTES.toMillis(1));

        GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(3L);

        assertEquals(GcOrphanStoreResult.Status.SKIPPED, result.status);
        assertTrue(new File(replDir(3L), ReplicationStoreDeleter.META_FILE).exists());
    }

    @Test
    public void testGcOrphanStoreDeletesMetaThenDataWhenOldEnough() throws Exception {
        writeMeta(4L, TimeUnit.DAYS.toMillis(2));

        GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(4L);

        assertEquals(GcOrphanStoreResult.Status.DELETED, result.status);
        assertFalse(replDir(4L).exists());
    }

    @Test
    public void testGcOrphanStoreSkipsWhenLiveKeeperOnThisContainer() throws Exception {
        writeMeta(5L, TimeUnit.DAYS.toMillis(2));
        Map<String, RedisKeeperServer> servers = new HashMap<>();
        servers.put(ReplId.from(5L).toString(), redisKeeperServer);
        ReflectionTestUtils.setField(keeperContainerService, "redisKeeperServers", servers);

        GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(5L);

        assertEquals(GcOrphanStoreResult.Status.SKIPPED, result.status);
        assertTrue(new File(replDir(5L), ReplicationStoreDeleter.META_FILE).exists());
    }

    @Test
    public void testGcOrphanStoreSkipsInBmMode() throws Exception {
        when(keeperContainerConfig.isTfsMode()).thenReturn(false);
        writeMeta(6L, TimeUnit.DAYS.toMillis(2));

        GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(6L);

        assertEquals(GcOrphanStoreResult.Status.SKIPPED, result.status);
        assertTrue(new File(replDir(6L), ReplicationStoreDeleter.META_FILE).exists());
    }

    private String metaPath(long replId) {
        return new File(replDir(replId), ReplicationStoreDeleter.META_FILE).getAbsolutePath();
    }

    private AsyncFile openMetaWriter(long replId) throws Exception {
        String path = metaPath(replId);
        return AsyncFileSystemHelper.awaitOpen(fs, () -> fs.open(path, AbstractStorageFile.OpenMode.READ_WRITE,
                AbstractStorageFile.ReplaceMode.ATOMIC, true, "test-writer"), "test open " + path);
    }

    /** While gc holds the meta write handle, a manager opening the store in this process must fail. */
    @Test
    public void testGcOrphanStoreBlocksWriterWhileDeleting() throws Exception {
        writeMeta(7L, TimeUnit.DAYS.toMillis(2));
        AsyncFileSystem spyFs = spy(fs);
        when(containerResourceManager.getAsyncFileSystem()).thenReturn(spyFs);
        AtomicReference<Throwable> writerOpenFailure = new AtomicReference<>();
        doAnswer(invocation -> {
            // runs after gc opened the meta file and before it deletes it
            try {
                AsyncFileSystemHelper.closeHandle(fs, openMetaWriter(7L), null, "unexpected writer");
            } catch (Throwable th) {
                writerOpenFailure.set(th);
            }
            return invocation.callRealMethod();
        }).when(spyFs).delete(org.mockito.ArgumentMatchers.any(AsyncFile.class));

        GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(7L);

        assertNotNull("writer must not open the meta file while gc holds it", writerOpenFailure.get());
        assertEquals(GcOrphanStoreResult.Status.DELETED, result.status);
        assertFalse(replDir(7L).exists());
    }

    /** A store whose meta file is already held by a writer (live manager) is left alone. */
    @Test
    public void testGcOrphanStoreSkipsWhenMetaHeldByWriter() throws Exception {
        writeMeta(8L, TimeUnit.DAYS.toMillis(2));
        AsyncFile writer = openMetaWriter(8L);
        try {
            GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(8L);

            assertEquals(GcOrphanStoreResult.Status.SKIPPED, result.status);
            assertTrue(new File(metaPath(8L)).exists());
        } finally {
            AsyncFileSystemHelper.closeHandle(fs, writer, null, "test close writer");
        }
    }

    /** Skipping must release the gc handle, so a manager can still open the store afterwards. */
    @Test
    public void testGcOrphanStoreReleasesHandleWhenSkipped() throws Exception {
        writeMeta(12L, TimeUnit.MINUTES.toMillis(1));

        GcOrphanStoreResult result = keeperContainerService.gcOrphanStore(12L);

        assertEquals(GcOrphanStoreResult.Status.SKIPPED, result.status);
        AsyncFileSystemHelper.closeHandle(fs, openMetaWriter(12L), null, "test close writer");
    }
}
