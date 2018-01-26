package com.ctrip.xpipe.redis.console.health.redisconf.diskless;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.HealthChecker;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConf;
import com.ctrip.xpipe.redis.console.health.redisconf.RedisConfManager;
import com.ctrip.xpipe.utils.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2017
 */

public class DiskLessCollectorTest {

    @InjectMocks
    private DefaultDiskLessCollector collector = new DefaultDiskLessCollector();

    @Mock
    private RedisConfManager confManager;

    @Mock
    private ConsoleConfig config;

    private List<String> diskLess;

    private String host = "127.0.0.1";
    private int port = 6379;
    private String clusterId = "test";
    private String shardId = "test";

    @Before
    public void beforeDiskLessCollectorTest() throws IOException {
        MockitoAnnotations.initMocks(this);
        diskLess = Arrays.asList("repl-diskless-sync", "yes");
    }

    @Test
    public void testCheckRedisVersion() {

        RedisConf redisConf = new RedisConf(new HostPort(host, port), clusterId, shardId);
        redisConf.setRedisVersion("3.0.7");
        redisConf.setXredisVersion("0.0.4");
        when(confManager.findOrCreateConfig(host, port)).thenReturn(redisConf);

        when(config.getReplDisklessMinRedisVersion()).thenReturn("2.8.22");

        HostPort hostPort = new HostPort(host, port);
        Assert.assertTrue(collector.isReplDiskLessSync(diskLess));
        Assert.assertFalse(collector.versionMatches(hostPort));
    }
}
