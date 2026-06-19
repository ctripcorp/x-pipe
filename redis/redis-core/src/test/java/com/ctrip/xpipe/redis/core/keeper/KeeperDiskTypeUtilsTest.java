package com.ctrip.xpipe.redis.core.keeper;

import org.junit.Assert;
import org.junit.Test;

public class KeeperDiskTypeUtilsTest {

    @Test
    public void testIsTfs() {
        Assert.assertFalse(KeeperDiskTypeUtils.isTfs(null));
        Assert.assertFalse(KeeperDiskTypeUtils.isTfs("DEFAULT"));
        Assert.assertFalse(KeeperDiskTypeUtils.isTfs("SSD-100"));
        Assert.assertTrue(KeeperDiskTypeUtils.isTfs("tfs"));
        Assert.assertTrue(KeeperDiskTypeUtils.isTfs("TFS-xxx"));
        Assert.assertTrue(KeeperDiskTypeUtils.isTfs("TfS-disk"));
    }
}
