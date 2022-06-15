package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.LongParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class SubscribeCommandHandler extends AbstractCommandHandler {
    @Override
    public String[] getCommands() {
        return new String[]{"subscribe"};
    }

    @Override
    protected void doHandle(String[] args, RedisClient redisClient) {
        if (args.length != 1) {
            redisClient.sendMessage(new RedisErrorParser("wrong format").format());
        }
        logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
        redisClient.sendMessage(String.format("*3%s", RedisProtocol.CRLF).getBytes());
        redisClient.sendMessage(String.format("$9%ssubscribe%s", RedisProtocol.CRLF, RedisProtocol.CRLF).getBytes());
        redisClient.sendMessage(String.format("$%d%s%s%s", args[0].length(), RedisProtocol.CRLF, args[0],
                RedisProtocol.CRLF).getBytes());
        redisClient.sendMessage(new LongParser(1L).format());
    }

    @Override
    public boolean isLog(String[] args) {
        return false;
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof RedisKeeperServer;
    }

}
