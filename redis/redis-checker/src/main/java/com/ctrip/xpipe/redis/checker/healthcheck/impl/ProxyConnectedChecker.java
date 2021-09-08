package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyPingCommand;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.KEYED_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class ProxyConnectedChecker implements ProxyChecker {
    
    @Autowired
    private CheckerConfig checkerConfig;
    
    @Resource(name = KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Resource(name = SCHEDULED_EXECUTOR)
    ScheduledExecutorService scheduled;
    
    @Override
    public CompletableFuture<Boolean> check(InetSocketAddress address) {
        CompletableFuture<Boolean> future = new CompletableFuture();
        ProxyPingCommand proxyPingCommand = new ProxyPingCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(address.getHostName(), address.getPort())), scheduled);
        CommandFuture commandFuture = proxyPingCommand.execute();
        commandFuture.addListener(new CommandFutureListener<ProxyPongEntity>() {
            @Override
            public void operationComplete(CommandFuture<ProxyPongEntity> commandFuture) throws Exception {
                if(commandFuture.isSuccess()) {
                    future.complete(true);
                } else {
                    future.complete(false);
                }
            }
        });
        return future;
    }

    @Override
    public int getRetryUpTimes() {
        return checkerConfig.getProxyCheckUpRetryTimes();
    }

    @Override
    public int getRetryDownTimes() {
        return checkerConfig.getProxyCheckDownRetryTimes();
    }
    
}
