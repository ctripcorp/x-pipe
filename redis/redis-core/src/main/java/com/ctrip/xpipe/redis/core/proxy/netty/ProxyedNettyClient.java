package com.ctrip.xpipe.redis.core.proxy.netty;

import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.netty.commands.DefaultNettyClient;
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
public class ProxyedNettyClient extends DefaultNettyClient {

    private static final Logger logger = LoggerFactory.getLogger(ProxyedNettyClient.class);

    private ChannelFuture future;

    public ProxyedNettyClient(ChannelFuture future) {
        super(future.channel());
        this.future = future;
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                desc.set(ChannelUtil.getDesc(future.channel()));
            }
        });
    }


    @Override
    public void sendRequest(ByteBuf byteBuf) {
        if(future.isSuccess()) {
            super.sendRequest(byteBuf);
        } else {
            logger.info("[channel not ready] send async-ly");
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    ProxyedNettyClient.super.sendRequest(byteBuf);
                }
            });
        }
    }

    @Override
    public void sendRequest(ByteBuf byteBuf, ByteBufReceiver byteBufReceiver) {
        if(future.isSuccess()) {
            super.sendRequest(byteBuf, byteBufReceiver);
        } else {
            logger.info("[channel not ready] send async-ly: {}", byteBufReceiver.getClass().getSimpleName());
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    ProxyedNettyClient.super.sendRequest(byteBuf, byteBufReceiver);
                }
            });
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
