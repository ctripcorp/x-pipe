package com.ctrip.xpipe.redis.protocal.cmd;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.ChannelCommandRequester;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.CommandRequester;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * request first command, process response; then next...
 * @author wenchao.meng
 *
 * May 11, 2016 6:20:26 PM
 */
public class DefaultCommandRequester implements CommandRequester{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultCommandRequester.class);

	private Map<Channel, ChannelCommandRequester>  commands = new ConcurrentHashMap<>();
	
	private ScheduledExecutorService scheduled;
	
	public DefaultCommandRequester(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}

	@Override
	public void request(Channel channel, Command command){

		if(!channel.isActive()){
			logger.warn("[request][channel inactive, discard command]" + channel + "," + command);
			return;
		}
		
		ChannelCommandRequester channelCommandRequester = getOrCreate(channel);
		channelCommandRequester.request(command);
	}


	private ChannelCommandRequester getOrCreate(Channel channel) {
		
		
		ChannelCommandRequester channelCommandRequester = commands.get(channel);
		if(channelCommandRequester == null){
			synchronized (commands) {
				channelCommandRequester = commands.get(channel);
				if(channelCommandRequester == null){
					channelCommandRequester = new SequenceChannelCommandRequester(channel, this);
					commands.put(channel, channelCommandRequester);
					
					channel.closeFuture().addListener(new ChannelFutureListener() {
						
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							
							if(logger.isInfoEnabled()){
								logger.info("[operationComplete][channel close, remove commands]" + future.channel());
							}
							commands.remove(future.channel());
						}
					});
					
				}
			}
		}
		return channelCommandRequester;
	}

	@Override
	public void schedule(TimeUnit timeUnit, int delay, final Channel channel, final Command command) {
		
		scheduled.schedule(new Runnable() {
			
			@Override
			public void run() {
				try {
					request(channel, command);
				} catch (Exception e) {
					logger.error("[run]" + channel + "," + command, e);
				}
			}
		}, delay, timeUnit);
	}

	@Override
	public void handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {
		
		ChannelCommandRequester channelCommandRequester = commands.get(channel);
		if(channel == null){
			logger.error("[handleResponse][no channel commands, check error]" + channel);
			throw new IllegalStateException("no channel commands" + channel);
		}
		channelCommandRequester.handleResponse(byteBuf);
	}

	@Override
	public void connectionClosed(Channel channel) {
		
		ChannelCommandRequester channelCommandRequester = commands.get(channel);
		if(channelCommandRequester == null){
			logger.warn("[handleResponse][no channel commands, check]" + channel);
			return;
		}
		
		channelCommandRequester.connectionClosed();
		
	}

}
