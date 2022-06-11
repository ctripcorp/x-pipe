package com.ctrip.xpipe.redis.keeper.netty;

import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.netty.ByteBufReadActionException;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author lishanglin
 * date 2022/6/11
 */
public class NettyApplierHandler extends AbstractNettyHandler {

    private ApplierServer applierServer;


    private CommandHandlerManager commandHandlerManager;

    private RedisClient<?> redisClient;

    public NettyApplierHandler(ApplierServer applierServer, CommandHandlerManager commandHandlerManager) {
        this.applierServer =  applierServer;
        this.commandHandlerManager = commandHandlerManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        redisClient = applierServer.clientConnected(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        applierServer.clientDisconnected(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {

            @Override
            public void read(Channel channel, ByteBuf byteBuf) throws ByteBufReadActionException {

                String []args = redisClient.readCommands(byteBuf);
                if(args != null) {
                    try {
                        commandHandlerManager.handle(args, redisClient);
                    } catch (Exception e) {
                        throw new ByteBufReadActionException(String.format("netty:%s, handle:%s", channel, StringUtil.join(",", args)), e);
                    }
                }

            }
        });

        super.channelRead(ctx, msg);
    }

}
