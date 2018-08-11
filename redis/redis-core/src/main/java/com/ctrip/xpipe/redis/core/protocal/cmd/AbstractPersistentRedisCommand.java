package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Aug 11, 2018
 */
public abstract class AbstractPersistentRedisCommand<T> extends AbstractRedisCommand<T> {

    public AbstractPersistentRedisCommand(String host, int port, ScheduledExecutorService scheduled) {
        super(host, port, scheduled);
    }

    public AbstractPersistentRedisCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public AbstractPersistentRedisCommand(String host, int port, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
        super(host, port, scheduled, commandTimeoutMilli);
    }

    public AbstractPersistentRedisCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
    }

    @Override
    protected void doExecute() throws CommandExecutionException {

        NettyClient nettyClient = null;
        try {
            logger.debug("[doExecute]{}", this);
            nettyClient = getClientPool().borrowObject();
            ByteBuf byteBuf = getRequest();
            doSendRequest(nettyClient, byteBuf);
        } catch (BorrowObjectException e) {
            throw new CommandExecutionException("execute " + this, e);
        }finally{
            afterCommandExecute(nettyClient);
        }
    }

    protected void afterCommandExecute(NettyClient nettyClient) {
        future().addListener(new CommandFutureListener<T>() {
            @Override
            public void operationComplete(CommandFuture<T> commandFuture) throws Exception {
                if(nettyClient != null) {
                    getClientPool().returnObject(nettyClient);
                }
                if(isPoolCreated()) {
                    LifecycleHelper.stopIfPossible(getClientPool());
                    LifecycleHelper.disposeIfPossible(getClientPool());
                }
            }
        });
    }
}
