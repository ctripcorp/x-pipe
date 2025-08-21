package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.payload.InOutPayloadFactory;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public abstract class AbstractSubscribe extends AbstractRedisCommand<Object> implements Subscribe {

    private static final int bulkStringInitSize = 1 << 5;

    private RedisClientProtocol<?> redisClientProtocol;

    private volatile SUBSCRIBE_STATE subscribeState = SUBSCRIBE_STATE.WAITING_RESPONSE;

    private MESSAGE_TYPE messageType;

    private String[] subscribeChannel;

    private AtomicInteger channelResponsed = new AtomicInteger(0);

    private List<SubscribeListener> listeners = Lists.newCopyOnWriteArrayList();

    private NettyClient nettyClient;

    protected AbstractSubscribe(String host, int port, ScheduledExecutorService scheduled,
                                Subscribe.MESSAGE_TYPE messageType, String... subscribeChannel) {
        super(host, port, scheduled);
        this.subscribeChannel = subscribeChannel;
        this.messageType = messageType;
        this.inOutPayloadFactory = new InOutPayloadFactory.DirectByteBufInOutPayloadFactory();
    }

    public AbstractSubscribe(String host, int port, ScheduledExecutorService scheduled, int commandTimeoutMilli,
                             MESSAGE_TYPE messageType, String... subscribeChannel) {
        super(host, port, scheduled, commandTimeoutMilli);
        this.messageType = messageType;
        this.subscribeChannel = subscribeChannel;
        this.inOutPayloadFactory = new InOutPayloadFactory.DirectByteBufInOutPayloadFactory();
    }

    public AbstractSubscribe(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                             MESSAGE_TYPE messageType, String... subscribeChannel) {
        super(clientPool, scheduled);
        this.messageType = messageType;
        this.subscribeChannel = subscribeChannel;
        this.inOutPayloadFactory = new InOutPayloadFactory.DirectByteBufInOutPayloadFactory();
    }

    public AbstractSubscribe(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli,
                             MESSAGE_TYPE messageType, String... subscribeChannel) {
        super(clientPool, scheduled, commandTimeoutMilli);
        this.messageType = messageType;
        this.subscribeChannel = subscribeChannel;
        this.inOutPayloadFactory = new InOutPayloadFactory.DirectByteBufInOutPayloadFactory();
    }

    @Override
    public void addChannelListener(SubscribeListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeChannelListener(SubscribeListener listener) {
        listeners.remove(listener);
    }

    @Override
    protected Object doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {

        if(!hasDataRead(byteBuf)){
            return null;
        }
        Object response = super.doReceiveResponse(channel, byteBuf);
        if (response == null) {
            return null;
        }
        return processResponse(channel, response);
    }

    private Object processResponse(Channel channel, Object response) {
        switch (subscribeState) {
            case SUBSCRIBING:
                // [message]-subscribeChannel-message  | [pmessage]-pchannel-subscribeChannel-message
                handleMessage(response);
                break;

            case WAITING_RESPONSE:
                handleResponse(channel, response);
                break;

            case UNSUBSCRIBE:
                unSubscribe();
                return response;
        }
        super.doReset();
        return null;
    }

    private boolean hasDataRead(ByteBuf byteBuf) {
        return byteBuf.readableBytes() > 0;
    }

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
                                getLogger().error("[doExecute]", e);
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

    @VisibleForTesting
    protected void handleResponse(Channel channel, Object response) {

        validateResponse(channel, response);
        if (channelResponsed.incrementAndGet() == subscribeChannel.length) {
            setSubscribeState(SUBSCRIBE_STATE.SUBSCRIBING);
        }
    }

    private void validateResponse(Channel channel, Object response) {
        if(!(response instanceof Object[])) {
            throw new RedisRuntimeException(String.format("Subscribe channel response incorrect: %s", response));
        }
        Object[] objects = (Object[]) response;

        if(objects.length < 3 || !messageType.isFromSubType(payloadToString(objects[0]))) {
            String message = String.format("Subscribe channel response incorrect: %s", Arrays.toString(objects));
            getLogger().error("[handleResponse]{}", message);
            throw new RedisRuntimeException(message);
        }

        if(logRequest()) {
            getLogger().info("[handleResponse][subscribe success]channel[{}]{}", channel, channel == null ? "unknown" : channel.attr(NettyClientHandler.KEY_CLIENT).get().toString());
        }
    }


    protected void doUnsubscribe() {
        getLogger().debug("[un-subscribe]close channel: {}",
                nettyClient == null ? "null - already closed" : ChannelUtil.getDesc(nettyClient.channel()));

        if(nettyClient != null && nettyClient.channel() != null) {
            nettyClient.channel().close();
        }
        if(!future().isDone()) {
            try {
                future().setSuccess();
            } catch (Throwable th) {
                getLogger().debug("[doUnsubscribe][future already done]", th);
            }
        }
    }

    protected void handleMessage(Object response) {
        if(!(response instanceof Object[])) {
            throw new RedisRuntimeException(String.format("Subscribe subscribeChannel response incorrect: %s", response));
        }
        SubscribeMessageHandler handler;
        if (this.getName().equals(PSUBSCRIBE)) {
            handler = getPSubscribeMessageHandler();
        } else {
            handler = getSubscribeMessageHandler();
        }

        Pair<String, String> channelAndMessage = handler.handle(payloadToStringArray(response));
        if(channelAndMessage != null) {
            notifyListeners(channelAndMessage);
        }
    }

    protected SubscribeMessageHandler getSubscribeMessageHandler() {
        return new DefaultSubscribeMessageHandler();
    }

    protected SubscribeMessageHandler getPSubscribeMessageHandler() {
        return new PsubscribeMessageHandler();
    }

    private void notifyListeners(Pair<String, String> channelAndMessage) {
        for(SubscribeListener listener : listeners) {
            try {
                listener.message(channelAndMessage.getKey(), channelAndMessage.getValue());
            } catch (Exception e) {
                getLogger().error("[notifyListeners] Listener: {}, exception: ", listener.getClass().getSimpleName(), e);
            }
        }
    }

    @Override
    protected Object format(Object payload) {
        return payload;
    }

    @Override
    public ByteBuf getRequest() {
        String[] request = new String[subscribeChannel.length + 1];
        request[0] = getName();
        System.arraycopy(subscribeChannel, 0, request, 1, subscribeChannel.length);
        return new RequestStringParser(request).format();
    }

    protected String[] getSubscribeChannel() {
        return subscribeChannel;
    }

    protected SUBSCRIBE_STATE getSubscribeState() {
        return subscribeState;
    }

    @VisibleForTesting
    protected synchronized void setSubscribeState(SUBSCRIBE_STATE state) {
        // WAITING_RESPONSE -> SUBSCRIBING means that subscriber has received subscribe header
        if (SUBSCRIBE_STATE.WAITING_RESPONSE == this.subscribeState && SUBSCRIBE_STATE.SUBSCRIBING == state) {
            cancelTimeout();
        }
        this.subscribeState = state;
    }

    @Override
    public void unSubscribe() {
        setSubscribeState(SUBSCRIBE_STATE.UNSUBSCRIBE);
        doUnsubscribe();
    }

    @Override
    protected void handleTimeout(NettyClient nettyClient) {
        super.handleTimeout(nettyClient);
        unSubscribe();
    }
}
