package com.ctrip.xpipe.redis.console.healthcheck.redis;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.healthcheck.BaseContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveInfo;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultRedisContext extends BaseContext implements RedisContext {

    private RedisInfo redisInfo;

    private static final int BACKOFF_CAP = 10;

    public DefaultRedisContext(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance) {
        super(scheduled, instance);
    }


    @Override
    public boolean isMater() {
        if(redisInfo == null) {
            return false;
        }
        return redisInfo.getRole().equals(Server.SERVER_ROLE.MASTER);
    }

    @Override
    public long getReplOffset() {
        if(redisInfo == null) {
            throw new IllegalStateException("Redis Info not set yet");
        }
        if(redisInfo instanceof MasterInfo) {
            return ((MasterInfo) redisInfo).getMasterReplOffset();
        } else if(redisInfo instanceof SlaveInfo) {
            return ((SlaveInfo) redisInfo).getSlaveReplOffset();
        }
        return 0;
    }

    @Override
    public Server.SERVER_ROLE getRole() {
        return redisInfo.getRole();
    }

    @Override
    public void refresh() {
        refresh(0);
    }

    @Override
    protected void doScheduledTask() {
        getRedisInfo();
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                getRedisInfo().addListener(new CommandFutureListener<RedisInfo>() {
                    @Override
                    public void operationComplete(CommandFuture<RedisInfo> commandFuture) throws Exception {
                        refresh();
                    }
                });
            }
        }, getWarmupTime(), TimeUnit.MILLISECONDS);
    }

    private void refresh(int attempts) {
        int timeout = 2 << attempts;
        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                getRedisInfo().addListener(new CommandFutureListener<RedisInfo>() {
                    @Override
                    public void operationComplete(CommandFuture<RedisInfo> commandFuture) throws Exception {
                        if(!commandFuture.isSuccess()) {
                            refresh(Math.min(BACKOFF_CAP, attempts + 1));
                        }
                    }
                });
            }
        }, timeout, TimeUnit.MILLISECONDS);

    }

    private CommandFuture<RedisInfo> getRedisInfo() {
        CommandFuture<RedisInfo> future = instance.getRedisSession().getRedisInfo();
        future.addListener(new CommandFutureListener<RedisInfo>() {
            @Override
            public void operationComplete(CommandFuture<RedisInfo> commandFuture) throws Exception {
                if(commandFuture.isSuccess()) {
                    redisInfo = commandFuture.get();
                }
            }
        });
        return future;
    }

    public DefaultRedisContext setRedisInfo(RedisInfo redisInfo) {
        this.redisInfo = redisInfo;
        return this;
    }
}
