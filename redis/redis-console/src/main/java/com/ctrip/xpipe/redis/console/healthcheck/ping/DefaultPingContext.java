package com.ctrip.xpipe.redis.console.healthcheck.ping;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.redis.console.health.PingCallback;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.BaseContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthStatusManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.xpipe.redis.console.healthcheck.action.DefaultHealthStatusManager.PING_DOWN_AFTER_MILLI;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultPingContext extends BaseContext implements PingContext {

    private AtomicReference<PingStatus> pingStatus = new AtomicReference<>(PingStatus.Unknown);

    private volatile long lastPingTime = HealthCheckContext.TIME_UNSET;

    private volatile long lastPongTime = HealthCheckContext.TIME_UNSET;

    public DefaultPingContext(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance) {
        super(scheduled, instance);
    }

    @Override
    public long lastPingTimeMilli() {
        return lastPingTime;
    }

    @Override
    public long lastPongTimeMilli() {
        return lastPongTime;
    }

    @Override
    public boolean isHealthy() {
        return System.currentTimeMillis() - lastPongTimeMilli() < PING_DOWN_AFTER_MILLI;
    }

    @Override
    public PingStatus getPingStatus() {
        return pingStatus.get();
    }

    @Override
    protected void doScheduledTask() {
        //setup
        if(lastPongTime == HealthCheckContext.TIME_UNSET) {
            lastPongTime = System.currentTimeMillis();
            lastPingTime = System.currentTimeMillis();
        }
        RedisSession session = instance.getRedisSession();
        CommandFuture<String> future = session.ping(new PingCallback() {
            @Override
            public void pong(String pongMsg) {
                lastPongTime = System.currentTimeMillis();
                setPingStatus(pingStatus.get().afterSuccess());
            }

            @Override
            public void fail(Throwable th) {
                setPingStatus(pingStatus.get().afterFail());
            }
        });
        future.addListener(new CommandFutureListener<String>() {
            @Override
            public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                lastPingTime = System.currentTimeMillis();
            }
        });
    }

    private void setPingStatus(PingStatus newStatus) {
        PingStatus oldStatus = this.pingStatus.getAndSet(newStatus);
        if(!newStatus.isHealthy() && !isHealthy()) {
            instance.markDown(HealthStatusManager.MarkDownReason.PING_FAIL);
        } else if(!oldStatus.isHealthy() && newStatus.isHealthy()) {
            instance.markUp(HealthStatusManager.MarkUpReason.PING_OK);
        }
    }
}
