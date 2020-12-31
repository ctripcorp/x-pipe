package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2020/12/28
 */
public class WrongClusterMetaException extends RedisConsoleException {

    public WrongClusterMetaException(String clusterName) {
        super("cluster meta not match for " + clusterName);
    }

}
