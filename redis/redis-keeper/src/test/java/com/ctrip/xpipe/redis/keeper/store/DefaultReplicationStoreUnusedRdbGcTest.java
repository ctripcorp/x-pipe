package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DefaultReplicationStoreUnusedRdbGcTest extends AbstractRedisKeeperTest {

    private AsyncFileSystem fs;
    private File baseDir;
    private TestKeeperConfig config;
    private final String runid = randomKeeperRunid();
    private DefaultReplicationStore store;

    @Before
    public void setUpStore() throws IOException {
        fs = createTestAsyncFileSystem();
        baseDir = new File(getTestFileDir());
        assertTrue(baseDir.isDirectory() || baseDir.mkdirs());
        config = new TestKeeperConfig();
        store = openStore();
    }

    @After
    public void tearDownStore() throws Exception {
        if (store != null) {
            try {
                store.close();
            } catch (Exception e) {
                logger.warn("[tearDownStore]", e);
            }
        }
        fs.shutdown();
    }

    @Override
    protected AsyncFileSystem asyncFileSystem() {
        return fs;
    }

    private DefaultReplicationStore openStore() throws IOException {
        DefaultReplicationStore opened = createDefaultReplicationStore(baseDir, config, runid, createkeeperMonitor(),
                mock(SyncRateManager.class), createRedisOpParser());
        opened.getMetaStore().becomeActive();
        return opened;
    }

    private File strayRdb(long ageMillis) throws IOException {
        File rdb = new File(baseDir, "rdb_" + System.nanoTime() + "_stray");
        Files.write(rdb.toPath(), new byte[]{1, 2, 3});
        assertTrue(rdb.setLastModified(System.currentTimeMillis() - ageMillis));
        return rdb;
    }

    @Test
    public void constructDoesNotDeleteUnusedRdb() throws Exception {
        File stray = strayRdb(3_600_000L);
        store.close();
        store = openStore();
        assertTrue("construct must leave unused rdb to gc", stray.exists());
    }

    @Test
    public void gcDeletesOldUnusedRdb() throws Exception {
        config.setMinTimeMilliToGcAfterCreate(60_000);
        File stray = strayRdb(3_600_000L);
        store.gc();
        assertFalse("old unused rdb removed by gc", stray.exists());
    }

    @Test
    public void gcKeepsRecentlyModifiedUnusedRdb() throws Exception {
        config.setMinTimeMilliToGcAfterCreate(60_000);
        File stray = strayRdb(0L);
        store.gc();
        assertTrue("recently modified rdb kept", stray.exists());
    }

    @Test
    public void gcKeepsRdbStillDumping() throws Exception {
        config.setMinTimeMilliToGcAfterCreate(0);
        DumpedRdbStore dumping = store.prepareNewRdb();
        File rdb = dumping.getRdbFile();
        assertTrue(rdb.exists());
        assertTrue(rdb.setLastModified(System.currentTimeMillis() - 3_600_000L));

        store.gc();
        assertTrue("dump in progress must not be deleted", rdb.exists());
        dumping.close();
    }

    @Test
    public void discardedDumpDeletedByGc() throws Exception {
        DumpedRdbStore dumping = store.prepareNewRdb();
        File rdb = dumping.getRdbFile();
        assertTrue(rdb.exists());

        store.discardDumpedRdb(dumping);
        assertTrue("discard only hands the rdb to gc", rdb.exists());
        store.gc();
        assertFalse("failed dump removed by gc", rdb.exists());
    }

    @Test
    public void discardedDumpKeptByGcWhileReferenced() throws Exception {
        DumpedRdbStore dumping = store.prepareNewRdb();
        File rdb = dumping.getRdbFile();
        dumping.incrementRefCount();

        store.discardDumpedRdb(dumping);
        store.gc();
        assertTrue("rdb still read by a slave is kept", rdb.exists());

        dumping.decrementRefCount();
        store.gc();
        assertFalse(rdb.exists());
    }

    @Test
    public void retryPrepareMovesGivenUpDumpToPrevious() throws Exception {
        DumpedRdbStore first = store.prepareNewRdb();
        DumpedRdbStore second = store.prepareNewRdb();

        store.gc();
        assertFalse("given-up dump removed by gc", first.getRdbFile().exists());
        assertTrue("new dump kept", second.getRdbFile().exists());
        second.close();
    }

    @Test
    public void discardOfUnknownRdbIsNoop() throws Exception {
        DumpedRdbStore dumping = store.prepareNewRdb();
        DumpedRdbStore other = store.prepareNewRdb();
        store.discardDumpedRdb(dumping);
        store.discardDumpedRdb(other);
        store.discardDumpedRdb(other);
        store.gc();
        assertFalse(dumping.getRdbFile().exists());
        assertFalse(other.getRdbFile().exists());
    }

    @Test(expected = IllegalStateException.class)
    public void adoptAfterDiscardRefused() throws Exception {
        DumpedRdbStore dumping = store.prepareNewRdb();
        store.discardDumpedRdb(dumping);
        store.checkReplIdAndUpdateRdb(dumping);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void discardReleasesFailedCurrentRdb() throws Exception {
        DumpedRdbStore dumping = store.prepareNewRdb();
        File rdb = dumping.getRdbFile();
        ((AtomicReference<RdbStore>) ReflectionTestUtils.getField(store, "rdbStoreRef")).set(dumping);
        ((AtomicReference<RdbStore>) ReflectionTestUtils.getField(store, "dumpingRdbRef")).set(null);

        store.discardDumpedRdb(dumping);
        assertNull("failed rdb released from current", store.getRdbStore());
        store.gc();
        assertFalse("failed rdb removed by gc", rdb.exists());
    }
}
