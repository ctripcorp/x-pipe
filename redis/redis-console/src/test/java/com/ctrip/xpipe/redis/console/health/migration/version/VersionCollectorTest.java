package com.ctrip.xpipe.redis.console.health.migration.version;

import com.ctrip.xpipe.utils.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */

public class VersionCollectorTest {

    String serverInfo;

    @Before
    public void before() throws IOException {
        String path = "src/test/resources/InfoServer";
        InputStream ins = FileUtils.getFileInputStream(path);
        serverInfo =  IOUtils.toString(ins);
    }

    @Test
    public void collectTest() throws IOException {
        String EXPECTED_VERSION = "3.0.7";
        String version = DefaultVersionCollector.getRedisVersion(serverInfo);
        Assert.assertEquals(EXPECTED_VERSION, version);
    }

}
