package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2017
 */
public class ClusterNotFoundException extends RedisConsoleException{

    public ClusterNotFoundException(String clusterName) {
        super(String.format("cluster not found:%s", clusterName));
    }

    public ClusterNotFoundException(long clusterId) {
        super(String.format("cluster not found:%s", clusterId));
    }

}
