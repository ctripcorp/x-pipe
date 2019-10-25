package com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.rewrite;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.protocal.cmd.ConfigRewrite;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConfigRewriteCheckActionTest extends AbstractTest {

    @Test
    public void testConfigRewriteCommand() throws Exception {
        Server server = startServer("-ERR Only CONFIG GET is allowed during loading\r\n");
        try {
            ConfigRewrite command = new ConfigRewrite(
                    getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", server.getPort())),
                    scheduled,
                    50);
            CountDownLatch latch = new CountDownLatch(1);
            CommandFuture<String> future = command.execute();
            future.addListener(new CommandFutureListener<String>() {
                @Override
                public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                    latch.countDown();
                }
            });
            latch.await(1000, TimeUnit.MILLISECONDS);
            Assert.assertFalse(future.isSuccess());
            Assert.assertTrue(future.cause() instanceof RedisError);

        } finally {
            server.stop();
        }
    }


    @Test
    public void testIgnoreRedisLoadingError() throws Exception {
        Server server = startServer("-ERR Only CONFIG GET is allowed during loading\r\n");
        AlertManager alertManager = mock(AlertManager.class);
        doNothing().when(alertManager).alert(any(DefaultRedisInstanceInfo.class), any(ALERT_TYPE.class), anyString());
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance()
                .setRedisInstanceInfo(new DefaultRedisInstanceInfo("dc", "cluster", "shard", new HostPort(), "SHAJQ"))
                .setSession(new RedisSession(new DefaultEndPoint("localhost", server.getPort()), scheduled, getXpipeNettyClientKeyedObjectPool()));
        try {
            new ConfigRewriteCheckAction(scheduled, instance,
                    executors, alertManager).doTask();
            sleep(100);
        } finally {
            server.stop();
        }

        verify(alertManager, never()).alert(any(DefaultRedisInstanceInfo.class), any(ALERT_TYPE.class), anyString());

    }
}