package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveInfo;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 07, 2017
 */
public class InfoReplicationCommand extends AbstractRedisCommand<RedisInfo> {

    protected static final String ROLE_PREFIX = "role";

    public InfoReplicationCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public InfoReplicationCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                                  int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
    }

    @Override
    protected RedisInfo format(Object payload) {

        String info = payloadToString(payload);

        return formatInfoString(info);
    }

    @VisibleForTesting
    protected RedisInfo formatInfoString(String info) {

        String[] split = info.split("\\s+");
        Server.SERVER_ROLE role = null;
        for (String line : split) {
            String[] parts = line.split("\\s*:\\s*");
            if (parts.length != 2) {
                continue;
            }
            String key = parts[0].trim(), value = parts[1].trim();
            if (key.equalsIgnoreCase(ROLE_PREFIX)) {
                role = Server.SERVER_ROLE.of(value);
                break;
            }
        }

        if (role == null) {
            throw new RedisRuntimeException("info has no role included:" + info);
        }

        switch (role){
            case MASTER:
                return MasterInfo.fromInfo(split);
            case KEEPER:
                return SlaveInfo.fromInfo(split);
            case SLAVE:
                return SlaveInfo.fromInfo(split);
            default:
                throw new IllegalStateException("impossible to be here");
        }
    }

    @Override
    public ByteBuf getRequest() {
        return new InfoCommand(null, InfoCommand.INFO_TYPE.REPLICATION, null).getRequest();
    }
}
