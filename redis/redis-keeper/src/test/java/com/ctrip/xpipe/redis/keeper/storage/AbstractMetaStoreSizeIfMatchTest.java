package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.spy;

public class AbstractMetaStoreSizeIfMatchTest extends AbstractRedisKeeperTest {

    private TailCacheFileSystemTest.FaultyDelegate faulty;
    private TailCacheFileSystem tcf;
    private AsyncFileSystem fs;
    private File baseDir;
    private final String runid = randomKeeperRunid();
    private DefaultMetaStore store;

    @Before
    public void setUpFs() throws IOException {
        TrackingExecutor ioExecutor = new TrackingExecutor(Executors.newCachedThreadPool());
        faulty = new TailCacheFileSystemTest.FaultyDelegate(ioExecutor);
        tcf = spy(new TailCacheFileSystem(faulty, new TailCacheFileSystemConfig(), ioExecutor));
        fs = tcf;
        baseDir = new File(getTestFileDir());
        store = new DefaultMetaStore(baseDir, runid, fs, getReplId());
        store.initialize();
    }

    @After
    public void tearDownFs() throws IOException {
        faulty.release();
        store.close();
        fs.shutdown();
    }

    @Override
    protected AsyncFileSystem asyncFileSystem() {
        return fs;
    }

    private void setRdbFileDirect(String fileName, long size) {
        ReplicationStoreMeta meta = metaRef();
        meta.setRdbFile(fileName);
        meta.setRdbFileSize(size);
    }

    private void setRordbFileDirect(String fileName, long size) {
        ReplicationStoreMeta meta = metaRef();
        meta.setRordbFile(fileName);
        meta.setRordbFileSize(size);
    }

    @Test
    public void testSetRdbFileSizeIfMatchReturnsFalseWhenFilenameDiffers() throws Exception {
        setRdbFileDirect("rdb-A", -1);
        boolean result = store.setRdbFileSizeIfMatch("rdb-B", 1234);
        assertFalse("filename differs", result);
        assertEquals("size unchanged", -1, metaRef().getRdbFileSize());
    }

    @Test
    public void testSetRdbFileSizeIfMatchReturnsFalseWhenSizeNotMinusOne() throws Exception {
        setRdbFileDirect("rdb-A", 1000);
        boolean result = store.setRdbFileSizeIfMatch("rdb-A", 1234);
        assertFalse("size is not -1", result);
        assertEquals("size unchanged", 1000, metaRef().getRdbFileSize());
    }

    @Test
    public void testSetRdbFileSizeIfMatchReturnsFalseWhenRdbFileNull() throws Exception {
        setRdbFileDirect(null, -1);
        boolean result = store.setRdbFileSizeIfMatch("rdb-A", 1234);
        assertFalse("rdbFile is null", result);
    }

    @Test
    public void testSetRdbFileSizeIfMatchWritesWhenConditionsMatch() throws Exception {
        setRdbFileDirect("rdb-A", -1);
        boolean result = store.setRdbFileSizeIfMatch("rdb-A", 5678);
        assertTrue("conditions matched", result);
        assertEquals("size updated", 5678, metaRef().getRdbFileSize());
        assertEquals("file unchanged", "rdb-A", metaRef().getRdbFile());
    }

    @Test
    public void testSetRordbFileSizeIfMatchReturnsFalseWhenFilenameDiffers() throws Exception {
        setRordbFileDirect("rordb-A", -1);
        boolean result = store.setRordbFileSizeIfMatch("rordb-B", 1234);
        assertFalse("filename differs", result);
        assertEquals("size unchanged", -1, metaRef().getRordbFileSize());
    }

    @Test
    public void testSetRordbFileSizeIfMatchReturnsFalseWhenSizeNotMinusOne() throws Exception {
        setRordbFileDirect("rordb-A", 1000);
        boolean result = store.setRordbFileSizeIfMatch("rordb-A", 1234);
        assertFalse("size is not -1", result);
        assertEquals("size unchanged", 1000, metaRef().getRordbFileSize());
    }

    @Test
    public void testSetRordbFileSizeIfMatchReturnsFalseWhenRordbFileNull() throws Exception {
        setRordbFileDirect(null, -1);
        boolean result = store.setRordbFileSizeIfMatch("rordb-A", 1234);
        assertFalse("rordbFile is null", result);
    }

    @Test
    public void testSetRordbFileSizeIfMatchWritesWhenConditionsMatch() throws Exception {
        setRordbFileDirect("rordb-A", -1);
        boolean result = store.setRordbFileSizeIfMatch("rordb-A", 5678);
        assertTrue("conditions matched", result);
        assertEquals("size updated", 5678, metaRef().getRordbFileSize());
        assertEquals("file unchanged", "rordb-A", metaRef().getRordbFile());
    }

    @Test
    public void testReadOnlyStoreReturnsFalseForIfMatch() throws Exception {
        store.close();
        DefaultMetaStore reader = new DefaultMetaStore(baseDir, runid, fs, getReplId(), true);
        reader.initialize();
        try {
            boolean result = reader.setRdbFileSizeIfMatch("rdb-A", 1234);
            assertFalse("read-only store should return false", result);
        } finally {
            reader.close();
        }
    }

    @Test
    public void testClosedStoreReturnsFalseForIfMatch() throws Exception {
        store.close();
        boolean result = store.setRdbFileSizeIfMatch("rdb-A", 1234);
        assertFalse("closed store should return false", result);
    }

    @Test
    public void testConcurrentMetaChangeReturnsFalse() throws Exception {
        setRdbFileDirect("rdb-A", -1);
        setRdbFileDirect("rdb-B", -1);
        boolean result = store.setRdbFileSizeIfMatch("rdb-A", 5678);
        assertFalse("meta changed after initial check", result);
    }

    @Test
    public void testIfMatchWriteReachesTfs() throws Exception {
        commitEofMarkRdb("rdb-A");

        assertTrue(store.setRdbFileSizeIfMatch("rdb-A", 5678));
        ReplicationStoreMeta persisted = metaFromFreshFs();
        assertEquals("rdb-A", persisted.getRdbFile());
        assertEquals(5678, persisted.getRdbFileSize());
    }

    @Test
    public void testIfMatchWriteFailureKeepsMetaRefAndRestoresCommitted() throws Exception {
        commitEofMarkRdb("rdb-A");
        ReplicationStoreMeta before = metaRef();
        faulty.failNext(TailCacheFileSystemTest.Op.FILE_WRITE, handle().path, new IOException("size-eio"));

        try {
            store.setRdbFileSizeIfMatch("rdb-A", 5678);
            fail("expected the size write to fail");
        } catch (IOException expected) {
            logger.info("[testIfMatchWriteFailureKeepsMetaRefAndRestoresCommitted] {}", expected.getMessage());
        }
        assertSame("metaRef unchanged", before, metaRef());
        assertEquals(-1, metaRef().getRdbFileSize());
        ReplicationStoreMeta persisted = metaFromFreshFs();
        assertEquals("rdb-A", persisted.getRdbFile());
        assertEquals("TFS holds the committed size", -1, persisted.getRdbFileSize());

        assertTrue("next check writes again", store.setRdbFileSizeIfMatch("rdb-A", 5678));
        assertEquals(5678, metaFromFreshFs().getRdbFileSize());
    }

    private void commitEofMarkRdb(String rdbFile) throws IOException {
        store.rdbBegun(randomKeeperRunid(), 1, rdbFile, new EofMarkType(randomKeeperRunid()), "cmd_test_");
        assertEquals(rdbFile, metaRef().getRdbFile());
        assertEquals(-1, metaRef().getRdbFileSize());
    }

    private ReplicationStoreMeta metaFromFreshFs() throws Exception {
        AsyncFileSystem fresh = createTestAsyncFileSystem();
        try {
            DefaultMetaStore reader = new DefaultMetaStore(baseDir, runid, fresh, getReplId(), true);
            reader.initialize();
            try {
                return reader.dupReplicationStoreMeta();
            } finally {
                reader.close();
            }
        } finally {
            fresh.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    private ReplicationStoreMeta metaRef() {
        return ((AtomicReference<ReplicationStoreMeta>)
                ReflectionTestUtils.getField(store, "metaRef")).get();
    }

    private AsyncFile handle() {
        return (AsyncFile) ReflectionTestUtils.getField(store, "metaAsyncFile");
    }
}