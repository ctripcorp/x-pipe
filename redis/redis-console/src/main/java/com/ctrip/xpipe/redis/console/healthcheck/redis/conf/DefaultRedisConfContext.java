package com.ctrip.xpipe.redis.console.healthcheck.redis.conf;

import com.ctrip.xpipe.redis.console.health.redisconf.Callbackable;
import com.ctrip.xpipe.redis.console.healthcheck.BaseContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoReplicationCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoReplicationComplementCommand;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultRedisConfContext extends BaseContext implements RedisConfContext {

    private String version;

    private String xversion;

    public DefaultRedisConfContext(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance) {
        super(scheduled, instance);
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public String getXRedisVersion() {
        return xversion;
    }

    @Override
    protected void doScheduledTask() {
        instance.getRedisSession().infoServer(new Callbackable<String>() {
            @Override
            public void success(String message) {

            }

            @Override
            public void fail(Throwable throwable) {

            }
        });
    }
}
