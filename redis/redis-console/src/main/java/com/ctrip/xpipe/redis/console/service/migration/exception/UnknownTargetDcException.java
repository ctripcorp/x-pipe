package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author lishanglin
 * date 2020/12/30
 */
public class UnknownTargetDcException extends RedisConsoleException {

    public UnknownTargetDcException(String clusterName, String targetDc) {
        super(String.format("unknown target dc %s for migration for %s", targetDc, clusterName));
    }

}
