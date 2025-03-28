package com.ctrip.xpipe.redis.keeper.handler.applier;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierStatistic;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

/**
 * @author lishanglin
 * date 2022/6/24
 */
public class ApplierInfoHandler extends AbstractCommandHandler {

    @Override
    public String[] getCommands() {
        return new String[]{"info"};
    }

    @Override
    protected void doHandle(String[] args, RedisClient<?> redisClient) {
        ApplierServer applierServer = (ApplierServer) redisClient.getRedisServer();
        Endpoint upstreamEndpoint = applierServer.getUpstreamEndpoint();
        ApplierStatistic statistic = applierServer.getStatistic();

        StringBuilder sb = new StringBuilder();
        sb.append("state:" + applierServer.getState() + RedisProtocol.CRLF);
        if (null != upstreamEndpoint) {
            sb.append("master_host:" + upstreamEndpoint.getHost() + RedisProtocol.CRLF );
            sb.append("master_port:"  + upstreamEndpoint.getPort() +  RedisProtocol.CRLF );
            sb.append("master_repl_offset:" + applierServer.getEndOffset() + RedisProtocol.CRLF);
            sb.append("drop_keys:" + statistic.getDroppedKeys() + RedisProtocol.CRLF);
            sb.append("trans_keys:" + statistic.getTransKeys() + RedisProtocol.CRLF);
        }

        redisClient.sendMessage(new CommandBulkStringParser(sb.toString()).format());
    }

    @Override
    public boolean support(RedisServer server) {
        return server instanceof ApplierServer;
    }

}
