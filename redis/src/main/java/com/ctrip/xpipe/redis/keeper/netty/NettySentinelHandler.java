package com.ctrip.xpipe.redis.keeper.netty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.cmd.SlaveOfCommand;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author marsqing
 *
 *         May 9, 2016 3:05:38 PM
 */
public class NettySentinelHandler extends ChannelDuplexHandler {

	private static Logger logger = LogManager.getLogger(NettySentinelHandler.class);

	private Map<Channel, Command> commands = new ConcurrentHashMap<Channel, Command>();

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {

		if (logger.isInfoEnabled()) {
			logger.info("[channelActive]" + ctx.channel());
		}

		SlaveOfCommand cmd = new SlaveOfCommand(ctx.channel());
		commands.put(ctx.channel(), cmd);
		cmd.request();

		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {

		if (logger.isInfoEnabled()) {
			logger.info("[channelInactive]" + ctx.channel());
		}

		commands.remove(ctx.channel());

		super.channelInactive(ctx);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("0X%X, %s", msg.hashCode(), msg.getClass()));
		}

		Command command = commands.get(ctx.channel());
		command.handleResponse((ByteBuf) msg);

		super.channelRead(ctx, msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

		logger.error("[exceptionCaught]" + ctx.channel(), cause);
		super.exceptionCaught(ctx, cause);
	}

}
