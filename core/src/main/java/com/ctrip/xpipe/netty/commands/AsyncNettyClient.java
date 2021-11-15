package com.ctrip.xpipe.netty.commands;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author chen.zhu
 * <p>
 * Aug 15, 2018
 */
public class AsyncNettyClient extends DefaultNettyClient {

    private static final Logger logger = LoggerFactory.getLogger(AsyncNettyClient.class);

    private ChannelFuture future;

    public AsyncNettyClient(ChannelFuture future, Endpoint endpoint) {
        super(future.channel());
        this.future = future;
        this.desc.set(String.format("L(%s)->R(%s:%d)", "UNKNOWN", endpoint.getHost(), endpoint.getPort()));
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    logger.info("[async][connected] endpint: {}, channel: {}", endpoint, ChannelUtil.getDesc(future.channel()));
                    if(endpoint instanceof ProxyEnabled) {
                        desc.set(String.format("%s, %s:%d", ChannelUtil.getDesc(future.channel()), endpoint.getHost(), endpoint.getPort()));
                    } else {
                        desc.set(ChannelUtil.getDesc(future.channel()));
                    }
                } else {
                    logger.info("[async][connect-fail] endpint: {}", endpoint, future.cause());
                }
            }
        });
        future.channel().closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("[async][closed] endpoint: {}, channel: {}", endpoint, ChannelUtil.getDesc(future.channel()));
            }
        });
    }


    @Override
    public void sendRequest(ByteBuf byteBuf) {
        if(future.channel() != null && future.channel().isActive()) {
            super.sendRequest(byteBuf);
        } else {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(future.isSuccess()) {
                        logger.info("[async][send][{}]", desc);
                        AsyncNettyClient.super.sendRequest(byteBuf);
                    } else {
                        logger.warn("[async][wont-send][{}]", desc);
                    }
                }
            });
        }
    }

    @Override
    public void sendRequest(ByteBuf byteBuf, ByteBufReceiver byteBufReceiver) {
        if(future.channel() != null && future.channel().isActive()) {
            super.sendRequest(byteBuf, byteBufReceiver);
        } else {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(future.isSuccess()) {
                        logger.info("[async][send][{}] {}", desc, byteBufReceiver.getClass().getSimpleName());
                        AsyncNettyClient.super.sendRequest(byteBuf, byteBufReceiver);
                    } else {
                        logger.warn("[async][wont-send][{}] {}", desc, byteBufReceiver.getClass().getSimpleName());
                        byteBufReceiver.clientClosed(AsyncNettyClient.this);
                    }
                }
            });
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
