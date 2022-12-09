package com.ctrip.xpipe.redis.keeper.handler.applier;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.protocal.ParserManager;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

/**
 * @author lishanglin
 * date 2022/6/24
 */
public class ApplierRoleCommandHandler extends AbstractCommandHandler {

    @Override
    public String[] getCommands() {
        return new String[]{"role"};
    }

    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) {
        ApplierServer applierServer = (ApplierServer) redisClient.getRedisServer();
        Endpoint upstreamEndpoint = applierServer.getUpstreamEndpoint();

        Object [] result = new Object[5];
        result[0] = applierServer.role().toString();
        result[1] = upstreamEndpoint == null ? "0.0.0.0": upstreamEndpoint.getHost();
        result[2] = upstreamEndpoint == null ? "0": upstreamEndpoint.getPort();
        result[3] = MASTER_STATE.REDIS_REPL_CONNECTED.getDesc();
        result[4] = 0;
        redisClient.sendMessage(ParserManager.parse(result));
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof ApplierServer;
    }

}
