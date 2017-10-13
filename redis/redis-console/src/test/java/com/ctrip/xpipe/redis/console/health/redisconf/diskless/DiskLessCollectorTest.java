package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.utils.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */

public class DiskLessCollectorTest extends AbstractConsoleIntegrationTest {

    String serverInfo;

    List<String> diskLess;

    @BeforeClass
    public static void beforeDiskLessCollectorTestClass() throws IOException {
        System.setProperty(HealthChecker.ENABLED, "true");
    }

    @Before
    public void beforeDiskLessCollectorTest() throws IOException {
        String path = "src/test/resources/InfoServer";
        InputStream ins = FileUtils.getFileInputStream(path);
        serverInfo = IOUtils.toString(ins);
        diskLess = Arrays.asList("repl-diskless-sync", "yes");
    }


    @Autowired
    private DefaultDiskLessCollector collector;

    @Test
    public void testCheckRedisVersion() {
        String clusterId = "test";
        String shardId = "test";
        HostPort hostPort = new HostPort("10.3.2.23", 10010);
        collector.checkRedisDiskLess(hostPort, diskLess, clusterId, shardId);
    }
}
