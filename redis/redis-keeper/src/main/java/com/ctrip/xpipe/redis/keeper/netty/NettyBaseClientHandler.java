package com.ctrip.xpipe.redis.keeper.netty;


import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.keeper.protocal.Command;
import com.ctrip.xpipe.redis.keeper.protocal.CommandRequester;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:05:38 PM
 */
public class NettyBaseClientHandler extends AbstractNettyHandler {

	private CommandRequester commandRequester;

	private Command initCmd;

	public NettyBaseClientHandler(CommandRequester commandRequester, Command initCmd) {
		this.commandRequester = commandRequester;
		this.initCmd = initCmd;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {

		commandRequester.request(ctx.channel(), initCmd);
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {

		commandRequester.connectionClosed(ctx.channel());
		super.channelInactive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("0X%X, %s", msg.hashCode(), msg.getClass()));
		}
		
		byteBufReadPolicy.read(ctx.channel(), (ByteBuf)msg, new ByteBufReadAction() {
			
			@Override
			public void read(Channel channel, ByteBuf byteBuf) throws XpipeException {
				commandRequester.handleResponse(channel, byteBuf);
			}
		});
		super.channelRead(ctx, msg);
	}

}
