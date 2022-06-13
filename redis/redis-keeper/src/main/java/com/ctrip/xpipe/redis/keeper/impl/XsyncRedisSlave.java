package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2022/6/7
 */
public class XsyncRedisSlave extends DefaultRedisSlave {

    private static final Logger logger = LoggerFactory.getLogger(XsyncRedisSlave.class);

    public XsyncRedisSlave(RedisClient redisClient) {
        super(redisClient);
    }

    protected String buildMarkBeforeFsync(ReplicationProgress<?, ?> rdbProgress) {
        return StringUtil.join(" ", DefaultXsync.FULL_SYNC, rdbProgress.getProgress().toString());
    }

    protected String buildThreadPrefix(Channel channel) {
        String getRemoteIpLocalPort = ChannelUtil.getRemoteAddr(channel);
        return  "RedisClientXsync-" + getRemoteIpLocalPort;
    }

    @Override
    public boolean supportProgress(ReplicationProgress.TYPE type) {
        return ReplicationProgress.TYPE.GTIDSET.equals(type);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
