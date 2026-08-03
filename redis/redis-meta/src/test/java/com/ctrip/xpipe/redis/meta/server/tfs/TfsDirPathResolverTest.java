package com.ctrip.xpipe.redis.meta.server.tfs;

import org.junit.Assert;
import org.junit.Test;

public class TfsDirPathResolverTest {

    @Test
    public void testResolveKeeperPortAndReplId() {
        String template = "/opt/data/100004376/rsd/replication_store_{keeper_port}/repl_{repl_id}";
        Assert.assertEquals("/opt/data/100004376/rsd/replication_store_6380/repl_42",
                TfsDirPathResolver.resolve(template, 6380, 42L));
    }

    @Test
    public void testResolveNullTemplate() {
        Assert.assertEquals("", TfsDirPathResolver.resolve(null, 6380, 42L));
    }
}
