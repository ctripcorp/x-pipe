package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public class SubscribeCommand extends AbstractSubscribe {

    public SubscribeCommand(String host, int port, ScheduledExecutorService scheduled, String channel) {
        super(host, port, scheduled, channel, MESSAGE_TYPE.MESSAGE);
    }

    public SubscribeCommand(Endpoint endpoint, ScheduledExecutorService scheduled, String channel) {
        super(endpoint.getHost(), endpoint.getPort(), scheduled, channel, MESSAGE_TYPE.MESSAGE);
    }

    public SubscribeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, String channel) {
        super(clientPool, scheduled, MESSAGE_TYPE.MESSAGE, channel);
    }

    @Override
    public void doUnsubscribe() {
        logger.info("[un-subscribe] set future to success");
        if(!future().isDone()) {
            future().setSuccess();
        }
    }

    @Override
    protected void afterCommandExecute(NettyClient nettyClient) {
        release();
        super.afterCommandExecute(nettyClient);
    }

    @Override
    public String getName() {
        return "subscribe";
    }
}
