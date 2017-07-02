package com.ctrip.xpipe.redis.console.service.migration.exception;

import com.ctrip.xpipe.redis.console.exception.RedisConsoleException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 29, 2017
 */
public class ClusterActiveDcNotRequest extends RedisConsoleException{

    public ClusterActiveDcNotRequest(String clusterName, String fromIdcRequest, String currentIdc) {
        super(String.format("cluster:%s, fromIdcRequest:%s, but current ActiveDc:%s", clusterName, fromIdcRequest, currentIdc));
    }
}
