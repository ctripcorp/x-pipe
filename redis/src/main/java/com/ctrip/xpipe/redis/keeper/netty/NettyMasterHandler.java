package com.ctrip.xpipe.redis.keeper.netty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettyMasterHandler extends ChannelDuplexHandler{
	
	private static Logger logger = LogManager.getLogger(NettyMasterHandler.class);
	
	private RedisKeeperServer redisKeeperServer;
	
	private CommandHandlerManager commandHandlerManager;
	
	private Map<Channel, RedisClient>  redisClients = new ConcurrentHashMap<Channel, RedisClient>(); 
	
	public NettyMasterHandler(RedisKeeperServer redisKeeperServer, CommandHandlerManager commandHandlerManager) {
		
		this.redisKeeperServer =  redisKeeperServer;
		this.commandHandlerManager = commandHandlerManager;
	}
	
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelActive]" + ctx.channel());
		}
		
		RedisClient redisClient = redisKeeperServer.clientConnected(ctx.channel());
		redisClients.put(ctx.channel(), redisClient);
		super.channelActive(ctx);
	}


	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		if(logger.isInfoEnabled()){
			logger.info("[channelInactive]" + ctx.channel());
		}

		redisKeeperServer.clientDisConnected(ctx.channel());
		redisClients.remove(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		if(logger.isDebugEnabled()){
			logger.debug(String.format("0X%X, %s", msg.hashCode(), msg.getClass()));
		}
		RedisClient redisClient = redisClients.get(ctx.channel());
		String []args= redisClient.readCommands((ByteBuf)msg);
		if(args != null){
			commandHandlerManager.handle(args, redisClient);;
		}
		super.channelRead(ctx, msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		
		logger.error("[exceptionCaught]" + ctx.channel(), cause);
		super.exceptionCaught(ctx, cause);
	}

}
