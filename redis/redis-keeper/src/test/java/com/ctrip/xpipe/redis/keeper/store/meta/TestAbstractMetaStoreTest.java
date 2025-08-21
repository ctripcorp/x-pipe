package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

/**
 * @author chen.zhu
 * <p>
 * May 19, 2020
 */
public class TestAbstractMetaStoreTest extends AbstractTest {

    private AbstractMetaStore metaStore = new DefaultMetaStore(new File("/tmp/xpipe/test"), "20180118165046194-20180118165046194-294c90b4c9ed4d747a77b1b0f22ec28a8068013b");

    private final String TMP_META_STORE_DIR = "/tmp/xpipe/test/";
    private final String TMP_META_JSON_FILE = TMP_META_STORE_DIR + "meta.json";
    private final String PERSIST_META_JSON_FILE = "src/test/resources/meta.json";

    @Before
    public void beforeTestAbstractMetaStoreTest() {
        File file = new File(TMP_META_JSON_FILE, "rw");
        file.delete();
    }

    @After
    public void afterTestAbstractMetaStoreTest() {
        File file = new File(TMP_META_JSON_FILE, "rw");
        file.delete();
    }

    @Test
    public void testSaveMetaToFile() throws Exception {
        ReplicationStoreMeta meta = metaStore.deserializeFromString(readFileAsString(PERSIST_META_JSON_FILE));
        metaStore.saveMetaToFile(new File(TMP_META_JSON_FILE), meta);
        metaStore = new DefaultMetaStore(new File("/tmp/xpipe/test"), "20180118165046194-20180118165046194-294c90b4c9ed4d747a77b1b0f22ec28a8068013b");
        metaStore.loadMeta();
        logger.info("[result] {}", readFileAsString(TMP_META_JSON_FILE));
        Assert.assertEquals(meta, metaStore.getMeta());
    }

    @Test
    public void testDeserializeFromString() {
        ReplicationStoreMeta actual = metaStore.deserializeFromString(readFileAsString("src/test/resources/meta.json"));
        ReplicationStoreMeta expected = new ReplicationStoreMeta();
        expected.setBeginOffset(539004786L);
        expected.setCmdFilePrefix("cmd_9116b193-9063-46f4-a677-04bcfe450171_");
        expected.setKeeperRunid("20180118165046194-20180118165046194-294c90b4c9ed4d747a77b1b0f22ec28a8068013b");
        expected.setKeeperState(KeeperState.BACKUP);
        expected.setMasterAddress(new DefaultEndPoint(new InetSocketAddress("10.2.24.215", 7379)));
        expected.setRdbFile("rdb_1589113559631_c58ed564-f393-4ca7-bb1e-9806e54a01bc");
        expected.setReplId("4d6bc3b1c0d9f147a5c905f4b9275095102d4efd");
        expected.setReplId2("0000000000000000000000000000000000000000");
        expected.setSecondReplIdOffset(-1L);
        expected.setRdbLastOffset(539004785L);
        expected.setRdbFileSize(178);
        Assert.assertEquals(expected, actual);
    }

//    @Test
//    public void testLoadMetaFromFile() {
//    }
//
//    @Test
//    public void testSaveMeta() {
//    }
//
//    @Test
//    public void testLoadMetaCreateIfEmpty() {
//    }
//
//    @Test
//    public void testSetRdbFileInfo() {
//    }
//
//    @Test
//    public void testGetMeta() {
//    }
//
//    @Test
//    public void testDoSetMasterAddress() {
//    }


}