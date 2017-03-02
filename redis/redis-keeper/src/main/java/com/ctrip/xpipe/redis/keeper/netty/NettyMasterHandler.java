package com.ctrip.xpipe.redis.keeper.netty;



import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.netty.ByteBufReadAction;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.handler.CommandHandlerManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午3:09:44
 */
public class NettyMasterHandler extends ChannelTrafficStatisticsHandler implements Observer{
	
	private RedisKeeperServer redisKeeperServer;
	
	private CommandHandlerManager commandHandlerManager;
	
	private static final AttributeKey<RedisClient> KEY_CLIENT = AttributeKey.newInstance(NettyMasterHandler.class.getSimpleName() + "_REDIS_CLIENTS");
	
	private int slaveListeningPort = -1;
	
	public NettyMasterHandler(RedisKeeperServer redisKeeperServer, CommandHandlerManager commandHandlerManager, long trafficReportIntervalMillis) {
		super(trafficReportIntervalMillis);
		this.redisKeeperServer =  redisKeeperServer;
		this.commandHandlerManager = commandHandlerManager;
	}
	
	
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		
		Channel channel = ctx.channel();
		RedisClient redisClient = redisKeeperServer.clientConnected(ctx.channel());
		redisClient.addObserver(this);
		getChannelRedisClient(channel).set(redisClient);
		super.channelActive(ctx);
	}

	private Attribute<RedisClient> getChannelRedisClient(Channel channel) {
		
		Attribute<RedisClient> client = channel.attr(KEY_CLIENT);
		return client;
		
	}


	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		
		redisKeeperServer.clientDisConnected(ctx.channel());
		super.channelInactive(ctx);
	}
	
	@Override
	protected void doChannelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		if(logger.isDebugEnabled()){
			logger.debug(String.format("0X%X, %s", msg.hashCode(), msg.getClass()));
		}

		final RedisClient redisClient = getChannelRedisClient(ctx.channel()).get();

		byteBufReadPolicy.read(ctx.channel(), (ByteBuf)msg, new ByteBufReadAction() {
			
			@Override
			public void read(Channel channel, ByteBuf byteBuf) throws XpipeException {
				
				String []args= redisClient.readCommands(byteBuf);
				if(args != null){
					commandHandlerManager.handle(args, redisClient);;
				}
				
			}
		});
	}


	@Override
	public void update(Object args, Observable observable) {
		
		if(args instanceof RedisSlave){
		    reportTraffic();
		    RedisSlave slave = (RedisSlave)args;
		    slaveListeningPort = slave.getSlaveListeningPort();
			logger.info("[update][become redis slave]" + args);
			Attribute<RedisClient> client = getChannelRedisClient(((RedisClient)observable).channel());
            client.set(slave);
		}
	}
	
	 @Override
    protected void doReportTraffic(long readBytes, long writtenBytes, String remoteIp, int remotePort) {
        if (writtenBytes > 0) {
            String type = String.format("Keeper.Out.%s", redisKeeperServer.getClusterId());
            String name = slaveListeningPort == -1
                    ? String.format("%s.client.%s:%s", redisKeeperServer.getShardId(), remoteIp, remotePort)
                    : String.format("%s.slave.%s:%s", redisKeeperServer.getShardId(), remoteIp, slaveListeningPort);
            EventMonitor.DEFAULT.logEvent(type, name, writtenBytes);
        }
    }


    @Override
    protected void doWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        
    }
}
