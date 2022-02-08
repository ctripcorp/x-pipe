package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.ReactorMetaServerConsoleServiceManager;
import com.ctrip.xpipe.utils.MapUtils;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lishanglin
 * date 2021/9/24
 */
public class DefaultReactorMetaServerConsoleServiceManager implements ReactorMetaServerConsoleServiceManager {

    private Map<String, ReactorMetaServerConsoleService> services = new ConcurrentHashMap<>();

    private LoopResources loopResources;

    private ConnectionProvider connectionProvider;

    public DefaultReactorMetaServerConsoleServiceManager(LoopResources loopResources, ConnectionProvider connectionProvider) {
        this.loopResources = loopResources;
        this.connectionProvider = connectionProvider;
    }

    @Override
    public ReactorMetaServerConsoleService getOrCreate(final String metaServerAddress) {
        return MapUtils.getOrCreate(services, metaServerAddress,
                () -> new DefaultReactorMetaServerConsoleService(metaServerAddress, loopResources, connectionProvider));
    }


}
