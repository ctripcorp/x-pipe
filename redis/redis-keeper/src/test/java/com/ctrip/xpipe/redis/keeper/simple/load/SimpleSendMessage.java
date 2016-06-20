package com.ctrip.xpipe.redis.keeper.simple.load;


import java.net.InetSocketAddress;

import com.ctrip.xpipe.redis.keeper.simple.AbstractLoadRedis;

import redis.clients.jedis.Jedis;


/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public class SimpleSendMessage extends AbstractLoadRedis{
	
	private final int messageSize = 1 << 10;
	
	private String message;
	
	public SimpleSendMessage(InetSocketAddress master) {
		super(master);
		message = createMessage(messageSize);
	}

	
	private String createMessage(int messageSize) {
		
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<messageSize;i++){
			sb.append('c');
		}
		return sb.toString();
	}


	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		total = 100;
		try(Jedis jedis = new Jedis(master.getHostName(), master.getPort())){
			for(int i=0; i < total; i++){
				jedis.set(String.valueOf(i), message);
			}
		}
		logger.info("[doStart][finish]");
		System.exit(0);
		
	}

	public static void main(String[] args) throws Exception {
		
		new SimpleSendMessage(
				new InetSocketAddress(6379)
				).start();

	}
	

}
