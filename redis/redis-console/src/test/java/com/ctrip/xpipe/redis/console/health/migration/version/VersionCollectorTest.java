package com.ctrip.xpipe.redis.console.health.migration.version;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */

public class VersionCollectorTest extends AbstractConsoleIntegrationTest {

    Logger logger = LoggerFactory.getLogger(getClass());

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

    @Autowired
    private DefaultVersionCollector collector;

    @Test
    public void testCheckRedisVersion() {
        String clusterId = "test";
        String shardId = "test";
        HostPort hostPort = new HostPort("10.3.2.23", 10010);
        String message = serverInfo;
        collector.checkRedisVersion(hostPort, message, clusterId, shardId);
    }
}
