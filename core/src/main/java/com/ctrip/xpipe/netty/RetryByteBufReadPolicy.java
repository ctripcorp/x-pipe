package com.ctrip.xpipe.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Jun 2, 2016
 */
public class RetryByteBufReadPolicy implements ByteBufReadPolicy{
	
	private Logger logger = LoggerFactory.getLogger(RetryByteBufReadPolicy.class);
	
	private int retry = 3;

	public RetryByteBufReadPolicy(){
	}

	public RetryByteBufReadPolicy(int retry){
		this.retry = retry;
	}
	
	
	@Override
	public void read(Channel channel, ByteBuf byteBuf, ByteBufReadAction byteBufReadAction) throws Exception {

		for(int i = 0; i < retry ; ){
			
			int before = byteBuf.readableBytes();
			byteBufReadAction.read(channel, byteBuf);
			int after = byteBuf.readableBytes();
			if( after <= 0 ){
				break;
			}
			if(after < before){
				//go on
				continue;
			}else if(after == before){
				i++;
			}else{
				logger.error("[channelRead][size increased after read!]{} < {}", before, after);
				break;
			}
		}
	}
}
