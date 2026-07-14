package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.replication.UnexpectedReplIdException;
import com.ctrip.xpipe.redis.keeper.container.ContainerResourceManager;
import com.ctrip.xpipe.redis.keeper.storage.AbstractStorageFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.utils.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.ctrip.xpipe.redis.core.store.MetaStore.META_V2_FILE;
import static com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta.DEFAULT_SECOND_REPLID_OFFSET;
import static com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta.EMPTY_REPL_ID;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * May 19, 2020
 */
public class DefaultMetaStoreTest extends AbstractRedisKeeperTest {
    private String replidA = "000000000000000000000000000000000000000A";
    private String replidB = "000000000000000000000000000000000000000B";
    private String replidC = "000000000000000000000000000000000000000C";
    private String masterUuidA = "111111111111111111111111111111111111111A";
    private String masterUuidB = "111111111111111111111111111111111111111B";
    private String masterUuidC = "111111111111111111111111111111111111111C";
    private String rdbFileA = "A.rdb";
    private String rdbFileB = "B.rdb";
    private String rdbFileC = "C.rdb";
    private String cmdPrefix = "cmd_prefidx_";
    private String baseDir;
    private String keeperRunId = "20180118165046194-20180118165046194-294c90b4c9ed4d747a77b1b0f22ec28a8068013b";
    private MetaStore metaStore;

    @Before
    public void beforeDefaultMetaTest() throws IOException {
        if (metaStore != null) {
            metaStore.close();
            metaStore = null;
        }
        baseDir = "/tmp/xpipe/test/" + currentTestName();
        Files.createDirectories(Paths.get(baseDir));
        AsyncFileSystem fs = asyncFileSystem();
        String metaPath = new File(baseDir, META_V2_FILE).getAbsolutePath();
        if (AsyncFileSystemHelper.await(fs.exists(metaPath), "check meta exists before test")) {
            AsyncFileSystemHelper.await(fs.delete(metaPath), "delete meta before test " + metaPath);
        }
        metaStore = new DefaultMetaStore(new File(baseDir), keeperRunId, fs, getReplId());
    }

    @After
    public void afterDefaultMetaTest() throws IOException {
        if (metaStore != null) {
            metaStore.close();
            metaStore = null;
        }
    }

    @Test
    public void saveMetaOpenV2WithAtomicReplace() throws IOException {
        metaStore.close();
        metaStore = null;
        AsyncFileSystem fileSystem = spy(ContainerResourceManager.createAsyncFileSystem(
                KeeperConfig.DEFAULT_ASYNC_IO_THREADS, KeeperConfig.DEFAULT_ASYNC_FSYNC_INTERVAL_BYTES));
        try {
            DefaultMetaStore store = new DefaultMetaStore(new File(baseDir), keeperRunId, fileSystem, getReplId());
            verify(fileSystem, times(1)).open(contains(META_V2_FILE), eq(AbstractStorageFile.OpenMode.READ_WRITE),
                    eq(true), eq(true), eq(getReplId().toString()));
            store.setRdbFileSize(1024);
            verify(fileSystem, times(1)).open(contains(META_V2_FILE), eq(AbstractStorageFile.OpenMode.READ_WRITE),
                    eq(true), eq(true), eq(getReplId().toString()));
            store.close();
        } finally {
            fileSystem.shutdown();
        }
    }

    @Test
    public void destroyClosesAndDeletesMetaFile() throws Exception {
        metaStore.close();
        DefaultMetaStore store = new DefaultMetaStore(new File(baseDir), keeperRunId, asyncFileSystem(), getReplId());
        File metaFile = new File(baseDir, META_V2_FILE);
        Assert.assertTrue(metaFile.exists());
        store.destroy();
        Assert.assertFalse(metaFile.exists());
        try {
            store.setRdbFileSize(1024);
            Assert.fail("expected IOException after destroy");
        } catch (IOException expected) {
            Assert.assertTrue(expected.getMessage().contains("MetaStore closed"));
        }
        try {
            store.loadMeta();
            Assert.fail("expected IOException after destroy");
        } catch (IOException expected) {
            Assert.assertTrue(expected.getMessage().contains("MetaStore closed"));
        }
    }

    @Test
    public void closeRejectsFurtherSave() throws IOException {
        metaStore.close();
        DefaultMetaStore store = new DefaultMetaStore(new File(baseDir), keeperRunId, asyncFileSystem(), getReplId());
        store.close();
        try {
            store.setRdbFileSize(1024);
            Assert.fail("expected IOException after close");
        } catch (IOException expected) {
            Assert.assertTrue(expected.getMessage().contains("MetaStore closed"));
        } finally {
            store.close();
        }
    }

    @Test
    public void closeRejectsFurtherLoad() throws IOException {
        metaStore.close();
        try {
            metaStore.loadMeta();
            Assert.fail("expected IOException after close");
        } catch (IOException expected) {
            Assert.assertTrue(expected.getMessage().contains("MetaStore closed"));
        }
    }

    @Test (expected = UnexpectedReplIdException.class)
    public void fixPsync0MakeSureReplIdsAreSame() throws IOException {
        metaStore.close();
        DefaultMetaStore spyStore = spy(new DefaultMetaStore(new File(baseDir), keeperRunId, asyncFileSystem(), getReplId()));
        try {
            spyStore.becomeActive();

            ReplicationStoreMeta meta = mock(ReplicationStoreMeta.class);
            when(meta.getReplId()).thenReturn("ReplId A");

            doReturn(meta).when(spyStore).dupReplicationStoreMeta();
            spyStore.checkReplIdAndUpdateRdbInfo("rdb_1620671301121_e67222d2-eee1-48c4-bde7-5c6d37734ca4", new EofMarkType("94480e125b6ebb54dc7b9eae7b9c8ea00aeed56e"), 572767153, "ReplId B");
        } finally {
            spyStore.close();
        }
    }

    @Test
    public void checkAndUpdateRdbInfoPsync() throws IOException {
        UPDATE_RDB_RESULT result;

        long beginReplOffsetA = 1, backlogOffA = 10000, rdbOffset;

        String rdbReplId = replidA, rdbFile = rdbFileA;

        metaStore.rdbConfirmPsync(replidA, beginReplOffsetA, backlogOffA, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);

        rdbOffset = 1500;
        result =  metaStore.checkReplIdAndUpdateRdbInfoPsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, 10000, 11000);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.RDB_MORE_RECENT);

        rdbOffset = 500;
        result =  metaStore.checkReplIdAndUpdateRdbInfoPsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, 10600, 11000);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.LACK_BACKLOG);

        rdbOffset = 500;
        result =  metaStore.checkReplIdAndUpdateRdbInfoPsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, 10000, 11000);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.OK);
    }

    @Test
    public void propertyNotMatch_checkAndUpdateRdbInfoXsyncFail() throws IOException {
        UPDATE_RDB_RESULT result;

        long beginReplOffsetA = 1, backlogOffA = 10000;

        GtidSet gtidContB = new GtidSet(GtidSet.EMPTY_GTIDSET);
        long beginReplOffsetB = 2001, backlogOffB = 11000;

        String rdbReplId = replidC, rdbMasterUuid = masterUuidC, rdbFile = rdbFileC;
        GtidSet rdbGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
        long rdbOffset = 2500, rdbBacklogOff = 11000;

        metaStore.rdbConfirmPsync(replidA, beginReplOffsetA, backlogOffA, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);

        result = metaStore.checkReplIdAndUpdateRdbInfoXsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, rdbMasterUuid, rdbGtidSet, rdbGtidSet, 10000, 11000, rdbBacklogOff, rdbGtidSet);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.REPLSTAGE_NOT_MATCH);

        metaStore.switchToXsync(replidB, beginReplOffsetB, backlogOffB, masterUuidB, gtidContB, null);

        result =  metaStore.checkReplIdAndUpdateRdbInfoXsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, rdbMasterUuid, rdbGtidSet, rdbGtidSet, 10000, 12000, rdbBacklogOff, rdbGtidSet);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.REPLID_NOT_MATCH);
    }

    @Test
    public void offsetNotMatch_checkAndUpdateRdbInfoXsyncFail() throws IOException {
        UPDATE_RDB_RESULT result;

        long beginReplOffsetA = 1, backlogOffA = 10000;

        GtidSet gtidContB = new GtidSet(GtidSet.EMPTY_GTIDSET);
        long beginReplOffsetB = 2001, backlogOffB = 11000;

        String rdbReplId = replidB, rdbMasterUuid = masterUuidB, rdbFile = rdbFileB;
        GtidSet rdbGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
        long rdbOffset, rdbBacklogOff;

        metaStore.rdbConfirmPsync(replidA, beginReplOffsetA, backlogOffA, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.switchToXsync(replidB, beginReplOffsetB, backlogOffB, masterUuidB, gtidContB, null);

        rdbOffset = 1500; rdbBacklogOff = 11500;
        result =  metaStore.checkReplIdAndUpdateRdbInfoXsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, rdbMasterUuid, rdbGtidSet, rdbGtidSet, 10000, 12000, rdbBacklogOff, rdbGtidSet);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.REPLOFF_OUT_RANGE);

        rdbOffset = 2500; rdbBacklogOff = 11500;
        result =  metaStore.checkReplIdAndUpdateRdbInfoXsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, rdbMasterUuid, rdbGtidSet, rdbGtidSet, 11600, 12000, rdbBacklogOff, rdbGtidSet);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.LACK_BACKLOG);

        rdbOffset = 3500; rdbBacklogOff = 12500;
        result =  metaStore.checkReplIdAndUpdateRdbInfoXsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, rdbMasterUuid, rdbGtidSet, rdbGtidSet, 10000, 12000, rdbBacklogOff, rdbGtidSet);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.RDB_MORE_RECENT);
    }

    @Test
    public void checkAndUpdateRdbInfoXsyncSucc() throws IOException {
        UPDATE_RDB_RESULT result;

        long beginReplOffsetA = 1, backlogOffA = 10000;

        GtidSet gtidContB = new GtidSet(GtidSet.EMPTY_GTIDSET);
        long beginReplOffsetB = 2001, backlogOffB = 11000;

        String rdbReplId = replidB, rdbMasterUuid = masterUuidB, rdbFile = rdbFileB;
        GtidSet rdbGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);
        long rdbOffset, rdbBacklogOff;

        metaStore.rdbConfirmPsync(replidA, beginReplOffsetA, backlogOffA, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.switchToXsync(replidB, beginReplOffsetB, backlogOffB, masterUuidB, gtidContB, null);

        rdbOffset = 2500; rdbBacklogOff = 11500;
        result =  metaStore.checkReplIdAndUpdateRdbInfoXsync(rdbFile, RdbStore.Type.NORMAL, new LenEofType(100), rdbOffset, rdbReplId, rdbMasterUuid, rdbGtidSet, rdbGtidSet, 10000, 12000, rdbBacklogOff, rdbGtidSet);
        Assert.assertEquals(result, UPDATE_RDB_RESULT.OK);
    }

    @Test
    public void testPsyncContinue() throws IOException{
        metaStore.rdbConfirmPsync(replidA, 1, 10000, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.psyncContinue(replidB, 11000);

        Assert.assertEquals(metaStore.getCurrentReplStage().getProto(), ReplStage.ReplProto.PSYNC);
        Assert.assertEquals(metaStore.getCurReplStageReplId(), replidB);
        Assert.assertEquals(metaStore.getCurrentReplStage().getReplId2(), replidA);
        Assert.assertEquals(metaStore.getCurrentReplStage().getSecondReplIdOffset(), 1001);
    }

    @Test
    public void testPsyncContinueReplidSame_Ignored() throws IOException{
        metaStore.rdbConfirmPsync(replidA, 1, 10000, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.psyncContinue(replidA, 11000);

        Assert.assertEquals(metaStore.getCurrentReplStage().getProto(), ReplStage.ReplProto.PSYNC);
        Assert.assertEquals(metaStore.getCurReplStageReplId(), replidA);
        Assert.assertEquals(metaStore.getCurrentReplStage().getReplId2(), EMPTY_REPL_ID);
        Assert.assertEquals(metaStore.getCurrentReplStage().getSecondReplIdOffset(), DEFAULT_SECOND_REPLID_OFFSET);
    }

    @Test
    public void testPsyncContinueFromOffset() throws IOException{
        metaStore.rdbConfirmPsync(replidA, 1, 10000, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.psyncContinueFrom(replidB, 10000, 20000, cmdPrefix);

        Assert.assertNull(metaStore.getPreReplStage());

        ReplStage replStage = metaStore.getCurrentReplStage();
        Assert.assertEquals(replStage.getProto(), ReplStage.ReplProto.PSYNC);
        Assert.assertEquals(metaStore.getCurReplStageReplId(), replidB);
        Assert.assertEquals(replStage.getSecondReplIdOffset(), DEFAULT_SECOND_REPLID_OFFSET);
        Assert.assertEquals(replStage.getReplId2(), EMPTY_REPL_ID);
        Assert.assertEquals(replStage.getBegOffsetRepl(), 10000);
        Assert.assertEquals(replStage.getBegOffsetBacklog(), 20000);

        ReplicationStoreMeta metaDup = metaStore.dupReplicationStoreMeta();
        Assert.assertNull(metaDup.getRdbFile());
    }

    @Test
    public void testSwitchToXsync() throws IOException{
        metaStore.rdbConfirmPsync(replidA, 1, 10000, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.switchToXsync(replidB, 20001,20000, masterUuidB, new GtidSet(GtidSet.EMPTY_GTIDSET), null);

        Assert.assertEquals(metaStore.getCurrentReplStage().getProto(), ReplStage.ReplProto.XSYNC);
        Assert.assertEquals(metaStore.getCurReplStageReplId(), replidB);
        Assert.assertEquals(metaStore.getPreReplStage().getProto(), ReplStage.ReplProto.PSYNC);
        Assert.assertEquals(metaStore.getPreReplStage().getReplId(), replidA);
    }

    @Test
    public void testXsyncContinue() throws IOException {
        metaStore.rdbConfirmXsync(replidA, 1, 10000, masterUuidA, new GtidSet(GtidSet.EMPTY_GTIDSET), new GtidSet(GtidSet.EMPTY_GTIDSET), rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        boolean updated = metaStore.xsyncContinue(replidB, 2001, 11000, masterUuidB, new GtidSet("A:1-100,B:1-100"), new GtidSet("A:1-100"));

        Assert.assertTrue(updated);
        Assert.assertEquals(metaStore.getPreReplStage(), null);
        ReplStage replStage = metaStore.getCurrentReplStage();
        Assert.assertEquals(replStage.getReplId(), replidB);
        Assert.assertEquals(replStage.getMasterUuid(), masterUuidB);
        Assert.assertEquals(replStage.getGtidLost(), new GtidSet("B:1-100"));
        Assert.assertEquals(replStage.getBegOffsetRepl(), 1001);
        Assert.assertEquals(replStage.getBegOffsetBacklog(), 10000);
    }

    @Test
    public void testSwitchToPsync() throws IOException {
        metaStore.rdbConfirmXsync(replidA, 1, 10000, masterUuidA, new GtidSet(GtidSet.EMPTY_GTIDSET), new GtidSet(GtidSet.EMPTY_GTIDSET), rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.switchToPsync(replidB, 2001, 11000);

        Assert.assertEquals(metaStore.getCurrentReplStage().getProto(), ReplStage.ReplProto.PSYNC);
        Assert.assertEquals(metaStore.getCurReplStageReplId(), replidB);
        Assert.assertEquals(metaStore.getPreReplStage().getProto(), ReplStage.ReplProto.XSYNC);
        Assert.assertEquals(metaStore.getPreReplStage().getReplId(), replidA);
    }

    @Test
    public void testXSyncProtoSaveAndLoad() throws IOException {
        metaStore.close();
        metaStore = null;
        AsyncFileSystem fileSystem = ContainerResourceManager.createAsyncFileSystem(
                KeeperConfig.DEFAULT_ASYNC_IO_THREADS, KeeperConfig.DEFAULT_ASYNC_FSYNC_INTERVAL_BYTES);
        try {
            MetaStore store = new DefaultMetaStore(new File(baseDir), keeperRunId, fileSystem, getReplId());
            store.rdbConfirmXsync(replidA, 1, 10000, masterUuidA,
                    new GtidSet(GtidSet.EMPTY_GTIDSET), new GtidSet(GtidSet.EMPTY_GTIDSET),
                    rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
            ReplStage expectedStage = store.getCurrentReplStage();
            store.close();
            MetaStore reloadedStore = new DefaultMetaStore(new File(baseDir), keeperRunId, fileSystem, getReplId());
            try {
                Assert.assertEquals(expectedStage, reloadedStore.getCurrentReplStage());
            } finally {
                reloadedStore.close();
            }
        } finally {
            fileSystem.shutdown();
        }
    }

    @Test
    public void testReplOffsetToBacklogOffset() throws Exception {
        metaStore.rdbConfirmPsync(replidA, 10001, 10000, rdbFileA, RdbStore.Type.NORMAL, new LenEofType(100), cmdPrefix);
        metaStore.switchToXsync(replidB, 20001,20000, masterUuidB, new GtidSet(GtidSet.EMPTY_GTIDSET), null);

        Assert.assertNull(metaStore.replOffsetToBacklogOffset(null));
        Assert.assertNull(metaStore.replOffsetToBacklogOffset(1L));
        Assert.assertNull(metaStore.replOffsetToBacklogOffset(9999L));
        Assert.assertEquals(metaStore.replOffsetToBacklogOffset(10000L), (Long)10000L);
        Assert.assertEquals(metaStore.replOffsetToBacklogOffset(10086L), (Long)10086L);
        Assert.assertEquals(metaStore.replOffsetToBacklogOffset(20000L), (Long)20000L);
        Assert.assertEquals(metaStore.replOffsetToBacklogOffset(20086L), (Long)20086L);
    }

    @Test
    public void testReplStageGtidNullIsEmpty() {
        ReplStage a = new ReplStage(replidA,10000,0,masterUuidA,null,null);
        ReplStage b = new ReplStage(replidA,10000,0,masterUuidA,new GtidSet(GtidSet.EMPTY_GTIDSET),new GtidSet(GtidSet.EMPTY_GTIDSET));
        Assert.assertEquals(a, b);
        a.setGtidLost(null);
        Assert.assertEquals(a.getGtidLost(), new GtidSet(GtidSet.EMPTY_GTIDSET));
    }
}
