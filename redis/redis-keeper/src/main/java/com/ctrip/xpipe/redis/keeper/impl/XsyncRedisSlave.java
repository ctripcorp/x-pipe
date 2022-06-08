package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author lishanglin
 * date 2022/6/7
 */
public class XsyncRedisSlave extends DefaultRedisSlave {

    public XsyncRedisSlave(RedisClient redisClient) {
        super(redisClient);
    }

    protected String buildMarkBeforeFsync(ReplicationProgress<?, ?> rdbProgress) {
        return StringUtil.join(" ", DefaultXsync.FULL_SYNC, rdbProgress.getProgress().toString());
    }

    @Override
    public boolean supportProgress(ReplicationProgress.TYPE type) {
        return ReplicationProgress.TYPE.GTIDSET.equals(type);
    }

}
