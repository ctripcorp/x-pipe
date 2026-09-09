package com.ctrip.xpipe.redis.comparator.config;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * T-MB.4 ①：缺省不抛错，默认值与 spec §5 逐项一致。
 */
public class ComparatorConfigTest extends AbstractTest {

    @Test
    public void testDefaultsMatchSpecWhenQConfigAbsent() {
        ComparatorConfig config = new ComparatorConfig();
        Assert.assertEquals("", config.getConsoleAddress());
        Assert.assertEquals(30000, config.getMetaRefreshIntervalMilli());
        Assert.assertEquals(2097152, config.getStreamBufferBytes());
        Assert.assertEquals(65536, config.getCompareChunkBytes());
        Assert.assertEquals(128, config.getMismatchDumpBytes());
        Assert.assertEquals("", config.getCmsAccessToken());
        Assert.assertEquals("", config.getCmsGetServerUrl());
    }

    @Test
    public void testDefaultConstantsEqualSpecNumbers() {
        Assert.assertEquals(2097152, ComparatorConfig.DEFAULT_STREAM_BUFFER_BYTES);
        Assert.assertEquals(30000, ComparatorConfig.DEFAULT_META_REFRESH_INTERVAL_MILLI);
        Assert.assertEquals(65536, ComparatorConfig.DEFAULT_COMPARE_CHUNK_BYTES);
        Assert.assertEquals(128, ComparatorConfig.DEFAULT_MISMATCH_DUMP_BYTES);
    }

    @Test
    public void testConstantsMatchSpec() {
        Assert.assertEquals(1000, ComparatorConstants.STREAM_RECONNECT_MIN_MILLI);
        Assert.assertEquals(30000, ComparatorConstants.STREAM_RECONNECT_MAX_MILLI);
        Assert.assertEquals(1000, ComparatorConstants.REPLCONF_ACK_INTERVAL_MILLI);
        Assert.assertEquals(100, ComparatorConstants.COMPARE_WAIT_MILLI);
        Assert.assertEquals(1000, ComparatorConstants.COMPARE_STOP_JOIN_MILLI);
        Assert.assertEquals(500, ComparatorConstants.COMPARE_THREAD_WARN_THRESHOLD);
        Assert.assertEquals(60000, ComparatorConstants.MISMATCH_LOG_MIN_INTERVAL_MILLI);
    }
}
