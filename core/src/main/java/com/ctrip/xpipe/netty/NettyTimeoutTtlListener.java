package com.ctrip.xpipe.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyTimeoutTtlListener implements NettyRequestListener {

    public static NettyRequestListener INSTANCE = new NettyTimeoutTtlListener();

    protected Logger logger = LoggerFactory.getLogger(getClass());

    Map<Channel, AtomicInteger> clientTimeoutCounters = new HashMap<>();

    protected static int CLIENT_TIMEOUT_TTL = 3;

    private NettyTimeoutTtlListener() {
    }

    @Override
    public void onSend(Channel channel, ByteBuf request) {
        if (!clientTimeoutCounters.containsKey(channel)) {
            initTimeoutCounter(channel);
        }
    }

    @Override
    public void onTimeout(Channel channel, int timeoutMilli) {
        AtomicInteger counter = clientTimeoutCounters.get(channel);
        if (null == counter) return;

        if (counter.incrementAndGet() >= CLIENT_TIMEOUT_TTL) handleTimeoutContinuously(channel);
    }

    @Override
    public void onReceive(Channel channel, ByteBuf response) {
        AtomicInteger counter = clientTimeoutCounters.get(channel);
        if (null == counter) return;

        counter.set(0);
    }

    private synchronized void initTimeoutCounter(Channel channel) {
        if (clientTimeoutCounters.containsKey(channel)) return;

        clientTimeoutCounters.put(channel, new AtomicInteger(0));
        channel.closeFuture().addListener(f -> clientTimeoutCounters.remove(channel));
    }

    private void handleTimeoutContinuously(Channel channel) {
        if (channel.isOpen()) {
            logger.info("[NettyTimeoutTtlListener] close channel {} for timeout {} times", channel, CLIENT_TIMEOUT_TTL);
            channel.close();
        }
    }

}
