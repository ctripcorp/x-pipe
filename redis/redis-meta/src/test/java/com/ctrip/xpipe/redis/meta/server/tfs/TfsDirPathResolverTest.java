package com.ctrip.xpipe.redis.meta.server.tfs;

import org.junit.Assert;
import org.junit.Test;

public class TfsDirPathResolverTest {

    @Test
    public void testResolveKeeperPort() {
        String template = "/opt/data/100004376/rsd/replication_store_{keeper_port}";
        Assert.assertEquals("/opt/data/100004376/rsd/replication_store_6380",
                TfsDirPathResolver.resolve(template, 6380));
    }

    @Test
    public void testResolveNullTemplate() {
        Assert.assertEquals("", TfsDirPathResolver.resolve(null, 6380));
    }
}
