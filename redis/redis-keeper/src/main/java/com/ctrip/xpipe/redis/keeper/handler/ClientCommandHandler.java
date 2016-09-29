package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class ClientCommandHandler extends AbstractCommandHandler {
    @Override
    public String[] getCommands() {
        return new String[]{"client"};
    }

    @Override
    protected void doHandle(String[] args, RedisClient redisClient) {
        if (args.length != 2) {
            redisClient.sendMessage(new RedisErrorParser("wrong format").format());
            return;
        }
        if (!args[0].equalsIgnoreCase("SETNAME")) {
            redisClient.sendMessage(new RedisErrorParser(String.format("%s not supported", args[0])).format());
            return;
        }
        logger.debug("[doHandle]{},{}", redisClient, StringUtil.join(" ", args));
        redisClient.sendMessage(SimpleStringParser.OK);
    }

    @Override
    public boolean isLog(String[] args) {
        return false;
    }
}
