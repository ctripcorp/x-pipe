package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2023/11/13
 */
public class DiskIOStatInfoTest extends AbstractTest {

    @Test
    public void testIOStatParse() {
        String raw = "sdb               0.00     0.00    0.00 1297.67     0.00  5600.00     8.63   141.17  100.66    0.00  100.66   0.77 100.00";
        DiskIOStatInfo info = DiskIOStatInfo.parse(raw);
        Assert.assertEquals("sdb", info.device);
        Assert.assertEquals(1297L, info.writeCnt);
        Assert.assertEquals(5600L, info.writeKB);
        Assert.assertEquals(8L, info.avgReqSize);
        Assert.assertEquals(141L, info.avgQueueSize);
        Assert.assertEquals(100.66, info.writeAwait, 0.01);
        Assert.assertEquals(100.66, info.await, 0.01);
        Assert.assertEquals(100.00, info.util, 0.01);
    }

}
