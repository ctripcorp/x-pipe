package com.ctrip.xpipe.redis.keeper.netty;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.netty.ByteBufReadActionException;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettyMasterHandler extends ChannelTrafficStatisticsHandler implements Observer{
	
	private RedisKeeperServer redisKeeperServer;
	
	private CommandHandlerManager commandHandlerManager;
	
	private RedisClient<RedisKeeperServer> redisClient;
	
	public NettyMasterHandler(RedisKeeperServer redisKeeperServer, CommandHandlerManager commandHandlerManager, long trafficReportIntervalMillis) {
		super(trafficReportIntervalMillis);
		this.redisKeeperServer =  redisKeeperServer;
		this.commandHandlerManager = commandHandlerManager;
	}
	
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		redisClient = redisKeeperServer.clientConnected(ctx.channel());
		redisClient.addObserver(this);
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		redisKeeperServer.clientDisconnected(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	protected void doChannelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		if(logger.isDebugEnabled()){
			logger.debug(String.format("0X%X, %s", msg.hashCode(), msg.getClass()));
		}
		ByteBuf byteBuf = (ByteBuf) msg;
		redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseInputBytes(byteBuf.readableBytes());
		byteBufReadPolicy.read(ctx.channel(), byteBuf, new ByteBufReadAction() {
			
			@Override
			public void read(Channel channel, ByteBuf byteBuf) throws ByteBufReadActionException {
				
				String []args= redisClient.readCommands(byteBuf);
				if(args != null){
					try {
						commandHandlerManager.handle(args, redisClient);
					} catch (Exception e) {
						throw new ByteBufReadActionException(String.format("netty:%s, handle:%s", channel, StringUtil.join(",", args)), e);
					}
				}
				
			}
		});
	}


	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof RedisSlave){
		    reportTraffic();
		    
		    RedisSlave slave = (RedisSlave)args;
		    redisClient = slave;
			logger.info("[update][become redis slave]{}", args);
		}
	}
	
	 @Override
    protected void doReportTraffic(long readBytes, long writtenBytes, String remoteIp, int remotePort) {
        if (writtenBytes > 0) {
            String type = String.format("Keeper.Out.%s", redisKeeperServer.getReplId());
            String name = null;
            if(redisClient instanceof RedisSlave){
            	RedisSlave slave = (RedisSlave)redisClient;
            	 name = String.format("slave.%s.%s.%s:%s", slave.roleDesc(), redisKeeperServer.getReplId(), remoteIp, slave.getSlaveListeningPort());
            }else{
            	name = String.format("client.%s.%s", redisKeeperServer.getReplId(), remoteIp);
            }
            EventMonitor.DEFAULT.logEvent(type, name, writtenBytes);
        }
    }


    @Override
    protected void doWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
		long writtenBytes = 0L;
		if (msg instanceof ByteBuf) {
			writtenBytes = ((ByteBuf) msg).readableBytes();
		} else if (msg instanceof FileRegion) {
			writtenBytes = (((FileRegion) msg).count());
		}
		redisKeeperServer.getKeeperMonitor().getKeeperStats().increaseOutputBytes(writtenBytes);
    }
}
