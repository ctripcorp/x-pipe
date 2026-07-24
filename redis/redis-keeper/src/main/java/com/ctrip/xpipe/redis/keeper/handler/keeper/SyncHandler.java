package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;
import com.ctrip.xpipe.utils.StringUtil;

import java.io.IOException;

/**
 * @author TB
 * @date 2026/7/24 13:44
 */
public class SyncHandler extends AbstractCommandHandler {
    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) throws Exception {
        logger.error("[doHandle][sync] {} ,{}" ,redisClient,StringUtil.join(" ", args));
        try {
            redisClient.close();
        } catch (IOException e) {
            logger.error("[doHandle] close " + redisClient, e);
        }
    }

    @Override
    public String[] getCommands() {
        return new String[] {"sync"};
    }
}
