package com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.version;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
public class VersionCheckActionTest extends AbstractConsoleTest {

    private VersionCheckAction action;

    private AlertManager alertManager;

    @Before
    public void beforeVersionCheckActionTest() throws Exception {
        alertManager = mock(AlertManager.class);
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("dc2", randomPort());
        action = new VersionCheckAction(scheduled, instance, executors, alertManager);
    }

    @Test
    public void testDoScheduledTask0Positive() {
        String info = "# Server\n" +
                "redis_version:4.0.8\n" +
                "xredis_version:1.0.1\n" +
                "redis_git_sha1:cd114f7e\n" +
                "redis_git_dirty:0\n" +
                "redis_build_id:c8fc0834f9ecab75\n" +
                "redis_mode:standalone\n" +
                "os:Darwin 17.5.0 x86_64\n" +
                "arch_bits:64\n";
        action.setInfo(info);
        action.doTask();
        Assert.assertTrue(action.isCheckPassed());
    }

    @Test
    public void testDoScheduledTask0Negative() {
        String info = "# Server\n" +
                "redis_version:2.8.19\n" +
                "redis_git_sha1:cd114f7e\n" +
                "redis_git_dirty:0\n" +
                "redis_build_id:c8fc0834f9ecab75\n" +
                "redis_mode:standalone\n" +
                "os:Darwin 17.5.0 x86_64\n" +
                "arch_bits:64\n";
        action.setInfo(info);
        action.doTask();
        Assert.assertFalse(action.isCheckPassed());
        verify(alertManager, times(1)).alert(any(), any(), any());
    }
}