package com.ctrip.xpipe.redis.console.keeper.Command;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.LoggableRedisCommand;
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

    protected InfoCommand generteInfoCommand(Endpoint key) {
        if(ProxyRegistry.getProxy(key.getHost(), key.getPort()) != null) {
            commandTimeOut = AbstractRedisCommand.PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI;
        }
        SimpleObjectPool<NettyClient> keyPool = keyedObjectPool.getKeyPool(key);
        return new InfoCommand(keyPool, InfoCommand.INFO_TYPE.REPLICATION.cmd(), scheduled, commandTimeOut);
    }

    protected <V> void addHookAndExecute(Command<V> command, Callbackable<V> callback) {
        logger.info("[zyfTest][addHookAndExecute] start execute");
        CommandFuture<V> future = command.execute();
        logger.info("[zyfTest][addHookAndExecute] start addListener");
        future.addListener(new CommandFutureListener<V>() {
            @Override
            public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()) {
                    logger.info("[zyfTest][addHookAndExecute] listener fail");
                    callback.fail(commandFuture.cause());
                } else {
                    logger.info("[zyfTest][addHookAndExecute] listener success");
                    callback.success(commandFuture.get());
                }
            }
        });
        try {
            logger.info("[zyfTest][addHookAndExecute] before get");
            future.get();
            logger.info("[zyfTest][addHookAndExecute] get over");
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}
