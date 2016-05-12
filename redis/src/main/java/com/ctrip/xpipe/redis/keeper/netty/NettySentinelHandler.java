package com.ctrip.xpipe.redis.keeper.netty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.protocal.CommandRequester;
import com.ctrip.xpipe.redis.protocal.cmd.SlaveOfCommand;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:05:38 PM
 */
public class NettySentinelHandler extends ChannelDuplexHandler {

	private static Logger logger = LogManager.getLogger(NettySentinelHandler.class);

	private CommandRequester commandRequester;
	
	public NettySentinelHandler(CommandRequester commandRequester) {
		this.commandRequester = commandRequester;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {

		if (logger.isInfoEnabled()) {
			logger.info("[channelActive]" + ctx.channel());
		}

		SlaveOfCommand cmd = new SlaveOfCommand();
		commandRequester.request(ctx.channel(), cmd);
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {

		if (logger.isInfoEnabled()) {
			logger.info("[channelInactive]" + ctx.channel());
		}
		commandRequester.connectionClosed(ctx.channel());
		super.channelInactive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("0X%X, %s", msg.hashCode(), msg.getClass()));
		}

		commandRequester.handleResponse(ctx.channel(), (ByteBuf)msg);
		super.channelRead(ctx, msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

		logger.error("[exceptionCaught]" + ctx.channel(), cause);
		super.exceptionCaught(ctx, cause);
	}

}
