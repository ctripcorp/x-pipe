package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.KeeperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
@Service
public class KeeperServiceImpl implements KeeperService {

    @Autowired
    MetaCache metaCache;

    @Autowired
    ConsoleConfig consoleConfig;

    @Override
    public boolean isKeeper(HostPort hostPort) {
        return metaCache.getAllKeepers().contains(hostPort);
    }
}
