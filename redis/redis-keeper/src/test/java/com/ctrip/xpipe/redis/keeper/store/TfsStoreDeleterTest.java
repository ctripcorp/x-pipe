package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** destroy path of {@link TfsStoreDeleter}: deletes only through the manager's handle, never opens meta. */
public class TfsStoreDeleterTest extends AbstractRedisKeeperTest {

    private static final ReplId REPL_ID = ReplId.from(42L);

    private AsyncFileSystem fs;
    private File baseDir;
    private String metaPath;

    @Before
    public void setUp() {
        fs = spy(createTestAsyncFileSystem());
        baseDir = new File(getTestFileDir(), REPL_ID.toString());
        assertTrue(baseDir.mkdirs());
        metaPath = new File(baseDir, ReplicationStoreDeleter.META_FILE).getAbsolutePath();
    }

    @After
    public void tearDown() throws Exception {
        fs.shutdown();
    }

    private AsyncFile openMetaWriter() throws Exception {
        AsyncFile meta = AsyncFileSystemHelper.awaitOpen(fs, () -> fs.open(metaPath, AbstractStorageFile.OpenMode.READ_WRITE,
                AbstractStorageFile.ReplaceMode.ATOMIC, true, REPL_ID.toString()), "test open " + metaPath);
        AsyncFileSystemHelper.writeAllBytes(fs, meta, "latest.store.dir=x".getBytes(StandardCharsets.UTF_8), "test write meta");
        return meta;
    }

    private void verifyNeverOpenedMeta() {
        verify(fs, never()).open(anyString(), any(AbstractStorageFile.OpenMode.class),
                any(AbstractStorageFile.ReplaceMode.class), anyBoolean(), any());
    }

    @Test
    public void testHeldHandleDeletesMetaThenDir() throws Exception {
        AsyncFile held = openMetaWriter();
        clearInvocations(fs);

        GcOrphanStoreResult result = new TfsStoreDeleter(fs, 0).delete(baseDir, REPL_ID, held);

        assertEquals(GcOrphanStoreResult.Status.DELETED, result.status);
        assertFalse(baseDir.exists());
        verify(fs).write(same(held), any());
        verify(fs).close(same(held));
        verifyNeverOpenedMeta();
    }

    @Test
    public void testHeldHandleNotWritableSkipsAndKeepsStore() throws Exception {
        AsyncFile held = openMetaWriter();
        doThrow(new IllegalStateException("lease lost")).when(fs).write(same(held), any());
        clearInvocations(fs);

        GcOrphanStoreResult result = new TfsStoreDeleter(fs, 0).delete(baseDir, REPL_ID, held);

        assertEquals(GcOrphanStoreResult.Status.SKIPPED, result.status);
        assertTrue(new File(metaPath).exists());
        verify(fs).close(same(held));
        verifyNeverOpenedMeta();
    }

    @Test
    public void testNoHandleWithMetaLeavesStoreToOrphanGc() throws Exception {
        AsyncFileSystemHelper.closeHandle(fs, openMetaWriter(), REPL_ID, "test close");
        clearInvocations(fs);

        GcOrphanStoreResult result = new TfsStoreDeleter(fs, 0).delete(baseDir, REPL_ID, null);

        assertEquals(GcOrphanStoreResult.Status.SKIPPED, result.status);
        assertTrue(new File(metaPath).exists());
        verifyNeverOpenedMeta();
    }

    @Test
    public void testNoHandleWithoutMetaRemovesDir() throws Exception {
        GcOrphanStoreResult result = new TfsStoreDeleter(fs, 0).delete(baseDir, REPL_ID, null);

        assertEquals(GcOrphanStoreResult.Status.DELETED, result.status);
        assertFalse(baseDir.exists());
        verifyNeverOpenedMeta();
    }
}
