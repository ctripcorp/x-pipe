package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.BorrowObjectException;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractPersistentRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public abstract class AbstractSubscribe extends AbstractPersistentRedisCommand<Object> implements Subscribe {

    private volatile SUBSCRIBE_STATE subscribeState = SUBSCRIBE_STATE.WAITING_RESPONSE;

    private MESSAGE_TYPE messageType;

    private String subscribeChannel;

    private List<SubscribeListener> listeners = Lists.newCopyOnWriteArrayList();

    protected AbstractSubscribe(String host, int port, ScheduledExecutorService scheduled, String subscribeChannel,
                                Subscribe.MESSAGE_TYPE messageType) {
        super(host, port, scheduled);
        this.subscribeChannel = subscribeChannel;
        this.messageType = messageType;
    }

    public AbstractSubscribe(String host, int port, ScheduledExecutorService scheduled, int commandTimeoutMilli,
                             MESSAGE_TYPE messageType, String subscribeChannel) {
        super(host, port, scheduled, commandTimeoutMilli);
        this.messageType = messageType;
        this.subscribeChannel = subscribeChannel;
    }

    public AbstractSubscribe(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                             MESSAGE_TYPE messageType, String subscribeChannel) {
        super(clientPool, scheduled);
        this.messageType = messageType;
        this.subscribeChannel = subscribeChannel;
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

        Object response = super.doReceiveResponse(channel, byteBuf);
        if(response == null) {
            return null;
        }

        logger.debug("[response] {}", response);

        switch (subscribeState) {
            case SUBSCRIBING:
                // [message]-subscribeChannel-message  | [pmessage]-pchannel-subscribeChannel-message
                handleMessage(response);
                break;

            case WAITING_RESPONSE:
                if(response == null) {
                    return null;
                }
                handleResponse(channel, response);
                break;

            case UNSUBSCRIBE:
                unSubscribe();
                return response;
        }
        super.doReset();
        return null;
    }


    @VisibleForTesting
    protected void handleResponse(Channel channel, Object response) {

        validateResponse(response);

        setSubscribeState(SUBSCRIBE_STATE.SUBSCRIBING);
    }

    private void validateResponse(Object response) {
        if(!(response instanceof Object[])) {
            throw new RedisRuntimeException(String.format("Subscribe channel response incorrect: %s", response));
        }
        Object[] objects = (Object[]) response;

        if(objects.length < 3 || !messageType.isFromSubType(payloadToString(objects[0]))) {
            String message = String.format("Subscribe channel response incorrect: %s", Arrays.toString(objects));
            logger.error("[handleResponse]{}", message);
            throw new RedisRuntimeException(message);
        }

        String monitorChannel = payloadToString(objects[1]);
        if(!ObjectUtils.equals(monitorChannel, getSubscribeChannel())) {
            String message = String.format("Subscribe channel: %s not as expected: %s", monitorChannel, getSubscribeChannel());
            logger.error("[handleResponse]{}", message);
            throw new RedisRuntimeException(message);
        }
        logger.info("[handleResponse] channel subscriber numbers: {} - {}", monitorChannel, payloadToLong(objects[2]));

    }


    protected abstract void doUnsubscribe();

    protected void handleMessage(Object response) {
        if(!(response instanceof Object[])) {
            throw new RedisRuntimeException(String.format("Subscribe subscribeChannel response incorrect: %s", response));
        }

        SubscribeMessageHandler handler = messageType.subscribeMessageHandler();

        Pair<String, String> channelAndMessage = handler.handle(payloadToStringArray(response));
        if(channelAndMessage != null) {
            notifyListeners(channelAndMessage);
        }
    }

    private void notifyListeners(Pair<String, String> channelAndMessage) {
        for(SubscribeListener listener : listeners) {
            try {
                listener.message(channelAndMessage.getKey(), channelAndMessage.getValue());
            } catch (Exception e) {
                logger.error("[notifyListeners] Listener: {}, exception: ", listener.getClass().getSimpleName(), e);
            }
        }
    }

    @Override
    protected Object format(Object payload) {
        return payload;
    }

    @Override
    public ByteBuf getRequest() {
        return new RequestStringParser(getName(), subscribeChannel).format();
    }

    @Override
    public int getCommandTimeoutMilli() {
        return 0;
    }


    protected String getSubscribeChannel() {
        return subscribeChannel;
    }

    protected SUBSCRIBE_STATE getSubscribeState() {
        return subscribeState;
    }

    private synchronized void setSubscribeState(SUBSCRIBE_STATE state) {
        this.subscribeState = state;
    }

    public void unSubscribe() {
        setSubscribeState(SUBSCRIBE_STATE.UNSUBSCRIBE);
        doUnsubscribe();
    }
}
