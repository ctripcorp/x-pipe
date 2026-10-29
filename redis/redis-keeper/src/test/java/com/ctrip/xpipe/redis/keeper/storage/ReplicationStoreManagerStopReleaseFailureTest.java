package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.AbstractCommandStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStoreManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/** keeper-tfs-m6 P4 (spec §3.2). */
public class ReplicationStoreManagerStopReleaseFailureTest extends AbstractRedisKeeperTest {

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private DeferredHandleCloserTest.ManualScheduler scheduler;
    private AsyncFileSystem realFs;
    private DeferredHandleCloser closer;
    private DefaultReplicationStoreManager manager;
    private ReplicationStore store;

    @Before
    public void setUpManager() throws Exception {
        scheduler = new DeferredHandleCloserTest.ManualScheduler(clock);
        realFs = spy(createTestAsyncFileSystem());
        closer = new DeferredHandleCloser(realFs, () -> 60_000L, clock::get, scheduler.executor);
        DeferredHandleCloser.register(realFs, closer);
        manager = new DefaultReplicationStoreManager(new TestKeeperConfig(), getReplId(), randomKeeperRunid(),
                new File(getTestFileDir()), createkeeperMonitor(), mock(SyncRateManager.class),
                createRedisOpParser(), realFs);
        LifecycleHelper.initializeIfPossible(manager);
        LifecycleHelper.startIfPossible(manager);
        store = manager.create();
        store.getMetaStore().becomeActive();
        RdbStore rdbStore = store.prepareRdb(randomKeeperRunid(), -1, new LenEofType(100));
        rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        rdbStore.updateRdbType(RdbStore.Type.NORMAL);
        store.confirmRdb(rdbStore);
    }

    @After
    public void tearDownManager() {
        try {
            LifecycleHelper.stopIfPossible(manager);
            manager.release();
            LifecycleHelper.disposeIfPossible(manager);
        } catch (Exception e) {
            logger.info("[tearDown] stop / dispose", e);
        }
        while (closer.count() > 0) {
            scheduler.runNext();
        }
        closer.shutdown(0);
        realFs.shutdown();
    }

    private AsyncSegmentFile cmdSegment() {
        return ((AbstractCommandStore) ReflectionTestUtils.getField(store, "cmdStore")).getWriteSegmentFile();
    }

    private AsyncFile storeMetaHandle() {
        return (AsyncFile) ReflectionTestUtils.getField(manager.getOpenedStore().getMetaStore(), "metaAsyncFile");
    }

    private AsyncFile managerMetaHandle() {
        return (AsyncFile) ReflectionTestUtils.getField(manager, "managerMetaAsyncFile");
    }

    private void failTwiceThenClose(AbstractStorageFile file) {
        OperationNotExecutedException one = new OperationNotExecutedException("in-flight");
        if (file instanceof AsyncSegmentFile) {
            doThrow(one).doThrow(one).doCallRealMethod().when(realFs).close(same((AsyncSegmentFile) file), anyBoolean());
        } else {
            doThrow(one).doThrow(one).doCallRealMethod().when(realFs).close(same((AsyncFile) file), anyBoolean());
        }
    }

    private void stopAndAssertFailure(AbstractStorageFile deferred) throws Exception {
        LifecycleHelper.stopIfPossible(manager);
        boolean releaseFailed = manager.release();
        assertTrue(manager.getLifecycleState().isPositivelyStopped());
        assertNull(manager.getOpenedStore());
        assertTrue(closer.isPending(deferred));

        assertTrue(releaseFailed);
    }

    @Test
    public void testDeferredCmdWriteHandleIsRecorded() throws Exception {
        AsyncSegmentFile segment = cmdSegment();
        failTwiceThenClose(segment);
        stopAndAssertFailure(segment);
    }

    @Test
    public void testDeferredStoreMetaWriteHandleIsRecorded() throws Exception {
        AsyncFile meta = storeMetaHandle();
        failTwiceThenClose(meta);
        stopAndAssertFailure(meta);
    }

    @Test
    public void testDeferredManagerMetaWriteHandleIsRecorded() throws Exception {
        AsyncFile managerMeta = managerMetaHandle();
        assertNotNull(managerMeta);
        failTwiceThenClose(managerMeta);
        stopAndAssertFailure(managerMeta);
    }

    @Test
    public void testDeferredRdbWriteHandleIsNotRecorded() throws Exception {
        AsyncFile rdbWrite = (AsyncFile) ReflectionTestUtils.getField(
                ((DefaultReplicationStore) store).getRdbStore(), "writeAsyncFile");
        assertNotNull(rdbWrite);
        failTwiceThenClose(rdbWrite);

        LifecycleHelper.stopIfPossible(manager);
        boolean releaseFailed = manager.release();
        assertTrue(manager.getLifecycleState().isPositivelyStopped());
        assertTrue(closer.isPending(rdbWrite));
        assertFalse(releaseFailed);
    }

    @Test
    public void testDeferredReadOnlyManagerMetaHandleIsNotRecorded() throws Exception {
        LifecycleHelper.stopIfPossible(manager);
        assertFalse(manager.release());
        manager.setReadOnly(true);
        LifecycleHelper.startIfPossible(manager);
        assertNotNull(manager.getCurrent());
        AsyncFile managerMeta = managerMetaHandle();
        assertFalse(managerMeta.canWrite());
        failTwiceThenClose(managerMeta);

        LifecycleHelper.stopIfPossible(manager);
        boolean releaseFailed = manager.release();
        assertTrue(closer.isPending(managerMeta));
        assertFalse(releaseFailed);
    }

    @Test
    public void testFailureOfEarlierStopIsNotReportedByNextStop() throws Exception {
        failTwiceThenClose(cmdSegment());
        LifecycleHelper.stopIfPossible(manager);
        manager.release();
        LifecycleHelper.startIfPossible(manager);
        LifecycleHelper.stopIfPossible(manager);
        assertFalse(manager.release());
    }

    @Test
    public void testReleaseOutsideStopReturnsFalseAndIsNotRecorded() throws Exception {
        AsyncFile meta = storeMetaHandle();
        failTwiceThenClose(meta);

        assertFalse(manager.releaseCurrentStore(false));
        assertNull("reference dropped even when a handle is deferred", manager.getOpenedStore());
        assertTrue(closer.isPending(meta));
        assertTrue("already released", manager.releaseCurrentStore(false));
    }
}
