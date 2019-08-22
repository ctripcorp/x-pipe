package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyClientHandler;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
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
public abstract class AbstractSubscribe extends AbstractRedisCommand<Object> implements Subscribe {

    private static final int bulkStringInitSize = 1 << 5;

    private RedisClientProtocol<?> redisClientProtocol;

    private volatile SUBSCRIBE_STATE subscribeState = SUBSCRIBE_STATE.WAITING_RESPONSE;

    private MESSAGE_TYPE messageType;

    private String[] subscribeChannel;

    private List<SubscribeListener> listeners = Lists.newCopyOnWriteArrayList();

    protected AbstractSubscribe(String host, int port, ScheduledExecutorService scheduled,
                                Subscribe.MESSAGE_TYPE messageType, String... subscribeChannel) {
        super(host, port, scheduled);
        this.subscribeChannel = subscribeChannel;
        this.messageType = messageType;
    }

    public AbstractSubscribe(String host, int port, ScheduledExecutorService scheduled, int commandTimeoutMilli,
                             MESSAGE_TYPE messageType, String... subscribeChannel) {
        super(host, port, scheduled, commandTimeoutMilli);
        this.messageType = messageType;
        this.subscribeChannel = subscribeChannel;
    }

    public AbstractSubscribe(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                             MESSAGE_TYPE messageType, String... subscribeChannel) {
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

        switch(commandResponseState){
            case READING_SIGN:
                if(!hasDataRead(byteBuf)){
                    return null;
                }
                int readIndex = byteBuf.readerIndex();
                byte sign = byteBuf.getByte(readIndex);
                if(sign == RedisClientProtocol.ASTERISK_BYTE){
                    commandResponseState = COMMAND_RESPONSE_STATE.READING_CONTENT;
                    redisClientProtocol = new ArrayParser(bulkStringInitSize);
                } else {
                    throw new IllegalArgumentException("subscribe should response with redis array format");
                }
            case READING_CONTENT:
                RedisClientProtocol<?> resultParser = redisClientProtocol.read(byteBuf);
                if(resultParser == null){
                    return null;
                }

                Object result = resultParser.getPayload();
                if(result == null){
                    return new String[0];
                }
                return processResponse(channel, result);
            default:
                throw new IllegalStateException("unkonwn state:" + commandResponseState);
        }

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
        future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
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

    @VisibleForTesting
    protected void handleResponse(Channel channel, Object response) {

        validateResponse(channel, response);

        setSubscribeState(SUBSCRIBE_STATE.SUBSCRIBING);
    }

    private void validateResponse(Channel channel, Object response) {
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
        for(String channelName : getSubscribeChannel()) {
            if (!ObjectUtils.equals(monitorChannel, channelName)) {
                String message = String.format("Subscribe channel: %s not as expected: %s", monitorChannel, channelName);
                logger.error("[handleResponse]{}", message);
                throw new RedisRuntimeException(message);
            }
        }
        if(logRequest()) {
            logger.info("[handleResponse][subscribe success], {}", channel.attr(NettyClientHandler.KEY_CLIENT).get().toString());
        }
    }


    protected abstract void doUnsubscribe();

    protected void handleMessage(Object response) {
        if(!(response instanceof Object[])) {
            throw new RedisRuntimeException(String.format("Subscribe subscribeChannel response incorrect: %s", response));
        }

        SubscribeMessageHandler handler = getSubscribeMessageHandler();

        Pair<String, String> channelAndMessage = handler.handle(payloadToStringArray(response));
        if(channelAndMessage != null) {
            notifyListeners(channelAndMessage);
        }
    }

    protected abstract SubscribeMessageHandler getSubscribeMessageHandler();

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
        String[] request = new String[subscribeChannel.length + 1];
        request[0] = getName();
        System.arraycopy(subscribeChannel, 0, request, 1, subscribeChannel.length);
        return new RequestStringParser(request).format();
    }

    @Override
    public int getCommandTimeoutMilli() {
        return 0;
    }

    protected String[] getSubscribeChannel() {
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
