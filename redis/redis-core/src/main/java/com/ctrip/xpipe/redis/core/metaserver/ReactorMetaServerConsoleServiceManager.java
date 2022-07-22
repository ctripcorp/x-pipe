package com.ctrip.xpipe.redis.core.metaserver;

/**
 * @author lishanglin
 * date 2021/9/24
 */
public interface ReactorMetaServerConsoleServiceManager {

    ReactorMetaServerConsoleService getOrCreate(MetaserverAddress metaserverAddress);

}
