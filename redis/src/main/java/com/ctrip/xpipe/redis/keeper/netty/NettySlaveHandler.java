package com.ctrip.xpipe.redis.keeper.netty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.protocal.Command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettySlaveHandler extends ChannelDuplexHandler{

	private Logger logger = LogManager.getLogger(NettySlaveHandler.class);
	
	private RedisKeeperServer redisKeeperServer;
	
	private Map<Channel, Command> commands = new ConcurrentHashMap<Channel, Command>();
	
	public NettySlaveHandler(RedisKeeperServer redisKeeperServer) {
		this.redisKeeperServer = redisKeeperServer;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		Channel channel = ctx.channel();

		if(logger.isInfoEnabled()){
			logger.info("[channelActive]" + channel);
		}
		
		Command command  = redisKeeperServer.slaveConnected(channel);
		commands.put(channel, command);
		command.request();
		super.channelActive(ctx);
	}

	
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelInactive]" + ctx.channel());
		}
		
		redisKeeperServer.slaveDisconntected(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		Command command = commands.get(ctx.channel());
		command.handleResponse((ByteBuf)msg);
		super.channelRead(ctx, msg);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		logger.error("[exceptionCaught]" + ctx.channel(), cause);
		super.exceptionCaught(ctx, cause);
	}

}
