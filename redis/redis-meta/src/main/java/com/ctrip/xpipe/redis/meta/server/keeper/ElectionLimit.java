package com.ctrip.xpipe.redis.meta.server.keeper;

/**
 * Created by yu
 * 2023/9/7
 */
public interface ElectionLimit {

    boolean tryAcquire(String shardKey);

}
