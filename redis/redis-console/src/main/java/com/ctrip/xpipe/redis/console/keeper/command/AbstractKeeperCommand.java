package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractKeeperCommand<V> extends AbstractCommand<V> {

    protected XpipeNettyClientKeyedObjectPool keyedObjectPool;

    protected ScheduledExecutorService scheduled;

    protected static final Logger logger = LoggerFactory.getLogger(AbstractKeeperCommand.class);

    private int commandTimeOut = Integer.parseInt(System.getProperty("KEY_REDISSESSION_COMMAND_TIMEOUT", String.valueOf(AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI)));

    protected AbstractKeeperCommand(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled) {
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
    }

    protected InfoCommand generateInfoReplicationCommand(Endpoint key) {
        SimpleObjectPool<NettyClient> keyPool = keyedObjectPool.getKeyPool(key);
        return new InfoCommand(keyPool, InfoCommand.INFO_TYPE.REPLICATION.cmd(), scheduled, commandTimeOut);
    }

}
