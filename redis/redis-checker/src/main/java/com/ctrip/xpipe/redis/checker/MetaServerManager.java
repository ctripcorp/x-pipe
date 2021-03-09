package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author lishanglin
 * date 2021/3/10
 */
public interface MetaServerManager {

    RedisMeta getCurrentMaster(String dcId, String clusterId, String shardId);

}
