package com.ctrip.xpipe.redis.proxy.monitor.stats.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyPingCommand;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 31, 2018
 */
public class DefaultPingStats extends AbstractStartStoppable implements PingStats {

    private Endpoint endpoint;

    private HostPort target;

    private volatile PingStatsResult result;

    private SimpleObjectPool<NettyClient> objectPool;

    private ScheduledExecutorService scheduled;

    private ScheduledFuture future;

    public DefaultPingStats(ScheduledExecutorService scheduled, Endpoint endpoint,
                            SimpleKeyedObjectPool<Endpoint, NettyClient> keyedObjectPool) {
        this.scheduled = scheduled;
        this.endpoint = endpoint;
        this.target = new HostPort(endpoint.getHost(), endpoint.getPort());
        this.objectPool = keyedObjectPool.getKeyPool(endpoint);
        this.result = new PingStatsResult(-1, -1, target, target);
    }

    @Override
    protected void doStart() throws Exception {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                doTask();
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
        }
    }

    @Override
    public Endpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public PingStatsResult getPingStatsResult() {
        return result;
    }

    protected void doTask() {
        long start = System.currentTimeMillis();
        new ProxyPingCommand(objectPool, scheduled).execute().addListener(commandFuture -> {
            if(!commandFuture.isSuccess()) {
                result = new PingStatsResult(start, Integer.MAX_VALUE, target, target);
            }
            ProxyPongEntity pong = commandFuture.getNow();
            if(pong == null) {
                return;
            }
            result = new PingStatsResult(start, System.currentTimeMillis(), target, pong.getDirect());
        });
    }

}
