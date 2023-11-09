package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.proxy.command.ProxyPingCommand;
import com.ctrip.xpipe.redis.core.proxy.command.entity.ProxyPongEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.KEYED_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
public class ProxyConnectedChecker implements ProxyChecker {
    Logger logger = LoggerFactory.getLogger(ProxyConnectedChecker.class);
    @Autowired
    private CheckerConfig checkerConfig;
    
    @Resource(name = KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Resource(name = SCHEDULED_EXECUTOR)
    ScheduledExecutorService scheduled;
    
    @Override
    public CompletableFuture<Boolean> check(InetSocketAddress address) {
        CompletableFuture<Boolean> future = new CompletableFuture();
        try {
            ProxyPingCommand proxyPingCommand = new ProxyPingCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(address.getHostName(), address.getPort())), scheduled);
            CommandFuture commandFuture = proxyPingCommand.execute();
            commandFuture.addListener((CommandFutureListener<ProxyPongEntity>) commandFuture1 -> {
                if(commandFuture1.isSuccess()) {
                    future.complete(true);
                } else {
                    future.complete(false);
                }
            });
        } catch (Exception proxyPingException) {
            logger.info("[proxyPingException] {}", proxyPingException);
            try {
                Socket socket = new Socket(address.getHostName(), address.getPort());
                future.complete(true);
                socket.close();
            } catch (Exception socketException) {
                logger.info("[socketException] {}", socketException);
                future.complete(false);
            }
        }
        
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
