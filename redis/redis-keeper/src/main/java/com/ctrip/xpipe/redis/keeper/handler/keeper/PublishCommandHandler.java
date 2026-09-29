package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.protocal.LongParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.redis.keeper.pubsub.KeeperPubSubRegistry;
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
        long receivers = 0L;
        if (args != null && args.length >= 2) {
            RedisKeeperServer server = (RedisKeeperServer) redisClient.getRedisServer();
            KeeperPubSubRegistry registry = server.getPubSubRegistry();
            if (registry != null) {
                receivers = registry.publish(args[0], args[1]);
            }
        }
        redisClient.sendMessage(new LongParser(receivers).format());
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
