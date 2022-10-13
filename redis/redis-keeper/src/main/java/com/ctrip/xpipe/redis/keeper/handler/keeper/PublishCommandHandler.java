package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.protocal.LongParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class PublishCommandHandler extends AbstractCommandHandler {

    @Override
    public String[] getCommands() {
        return new String[]{"publish"};
    }

    @Override
    protected void doHandle(String[] args, RedisClient redisClient) {
        logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
        //0 means no receiver
        redisClient.sendMessage(new LongParser(0L).format());
    }

    @Override
    public boolean isLog(String[] args) {
        // PUBLISH command is called by sentinel very frequently, so we need to hide the log
        return false;
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof RedisKeeperServer;
    }

}
