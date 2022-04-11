package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.MasterSlavesInfo;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoReplicationCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveInfo;

import java.util.concurrent.ScheduledExecutorService;

public class CheckMasterRoleAndSlaves extends InfoReplicationCommand {

    public CheckMasterRoleAndSlaves(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public CheckMasterRoleAndSlaves(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                                    int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
    }

    @Override
    protected RedisInfo formatInfoString(String info) {
        InfoResultExtractor extractor = new InfoResultExtractor(info);
        String roleString = extractor.extract(ROLE_PREFIX);
        if (roleString == null) {
            throw new RedisRuntimeException("info has no role included:" + info);
        }
        Server.SERVER_ROLE role = Server.SERVER_ROLE.of(roleString);
        return redisInfo(role, extractor);
    }


    protected RedisInfo redisInfo(Server.SERVER_ROLE role, InfoResultExtractor extractor) {
        switch (role) {
            case MASTER:
                return MasterSlavesInfo.fromInfo(extractor);
            case SLAVE:
                return new SlaveInfo();
            default:
                throw new IllegalStateException(String.format("role %s not supported in CheckMasterRoleAndSlaves", role));
        }
    }

}
