package com.ctrip.xpipe.redis.keeper.simple.load;

import java.net.InetSocketAddress;

import com.ctrip.xpipe.redis.keeper.simple.AbstractLoadRedis;

import redis.clients.jedis.Jedis;

/**
 * @author wenchao.meng
 *
 *         May 23, 2016
 */
public class SimpleSendMessage extends AbstractLoadRedis {

	private final int messageSize = 1 << 10;

	private String message;

	public SimpleSendMessage(InetSocketAddress master) {
		super(master);
		message = createMessage(messageSize);
	}

	private String createMessage(int messageSize) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < messageSize; i++) {
			sb.append('c');
		}
		return sb.toString();
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();

		try (Jedis jedis = new Jedis(master.getHostString(), master.getPort())) {
			while (true) {
				long index = increase();
				if (index < 0) {
					logger.info("[doStart][index < 0, break]{}", index);
					break;
				}
				long keyIndex = getKeyIndex(index);
				jedis.set(String.valueOf(keyIndex), message);
			}
		}
		logger.info("[doStart][finish]");
		System.exit(0);

	}

	public static void main(String[] args) throws Exception {

		SimpleSendMessage simpleSendMessage = new SimpleSendMessage(new InetSocketAddress(6379));
		simpleSendMessage.initialize();
		simpleSendMessage.start();

	}

}
