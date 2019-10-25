package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public class SubscribeCommand extends AbstractSubscribe {

    private NettyClient nettyClient;

    public SubscribeCommand(String host, int port, ScheduledExecutorService scheduled, String... channel) {
        super(host, port, scheduled, MESSAGE_TYPE.MESSAGE, channel);
    }

    public SubscribeCommand(Endpoint endpoint, ScheduledExecutorService scheduled, String... channel) {
        super(endpoint.getHost(), endpoint.getPort(), scheduled, MESSAGE_TYPE.MESSAGE, channel);
    }

    public SubscribeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, String... channel) {
        super(clientPool, scheduled, MESSAGE_TYPE.MESSAGE, channel);
    }

    @Override
    public void doUnsubscribe() {
        logger.debug("[un-subscribe]close channel: {}",
                nettyClient == null ? "null - already closed" : ChannelUtil.getDesc(nettyClient.channel()));

        if(nettyClient != null && nettyClient.channel() != null) {
            nettyClient.channel().close();
        }
        if(!future().isDone()) {
            future().setSuccess();
        }
    }

    @Override
    protected SubscribeMessageHandler getSubscribeMessageHandler() {
        return new DefaultSubscribeMessageHandler();
    }

    @Override
    protected void afterCommandExecute(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
        future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(nettyClient != null){
                    nettyClient.channel().close().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            try {
                                getClientPool().returnObject(nettyClient);
                            } catch (ReturnObjectException e) {
                                logger.error("[doExecute]", e);
                            }
                        }
                    });
                }

                if(isPoolCreated()){
                    LifecycleHelper.stopIfPossible(getClientPool());
                    LifecycleHelper.disposeIfPossible(getClientPool());
                }
            }
        });
    }

    @Override
    public String getName() {
        return "subscribe";
    }
}
