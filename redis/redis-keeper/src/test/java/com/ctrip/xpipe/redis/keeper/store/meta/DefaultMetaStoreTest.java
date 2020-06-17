package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 19, 2020
 */
public class DefaultMetaStoreTest extends AbstractTest {

    private DefaultMetaStore metaStore = new DefaultMetaStore(new File("/tmp/xpipe/test"), "19c9a31ab9aeb916429198e51e7abada9d9ecb62");

    @Before
    public void beforeDefaultMetaStoreTest() {

    }

    @Test
    public void testGetReplId() {
        metaStore.getReplId();
    }

    @Test
    public void testGetReplId2() {
    }

    @Test
    public void testGetSecondReplIdOffset() {
    }

    @Test
    public void testBeginOffset() {
    }

    @Test
    public void testDoSetMasterAddress() {
    }

    @Test
    public void testGetMasterAddress() {
    }

    @Test
    public void testRdbBegun() {
    }

    @Test
    public void testMasterChanged() {
    }

    @Test
    public void testClearReplicationId2() {
    }

    @Test
    public void testShiftReplicationId() {
    }
}