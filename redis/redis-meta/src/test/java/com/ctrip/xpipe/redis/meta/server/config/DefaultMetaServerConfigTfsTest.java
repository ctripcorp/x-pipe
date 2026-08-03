package com.ctrip.xpipe.redis.meta.server.config;

import org.junit.Assert;
import org.junit.Test;

public class DefaultMetaServerConfigTfsTest {

    @Test
    public void testDefaultHostAndDirPathAndAppId() {
        UnitTestServerConfig config = new UnitTestServerConfig();
        Assert.assertEquals(DefaultMetaServerConfig.DEFAULT_TFS_GATEWAY_HOST, config.getTfsGatewayHost());
        Assert.assertEquals(0L, config.getTfsGatewayAppId());
        Assert.assertEquals(DefaultMetaServerConfig.DEFAULT_TFS_DIR_PATH_TEMPLATE, config.getTfsDirPathTemplate());
        Assert.assertTrue(config.getTfsDirPathTemplate().contains("{repl_id}"));
        Assert.assertTrue(config.getTfsDirPathTemplate().contains("{keeper_port}"));
    }
}
