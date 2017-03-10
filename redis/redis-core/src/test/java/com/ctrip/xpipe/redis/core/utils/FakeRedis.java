package com.ctrip.xpipe.redis.core.utils;

import com.ctrip.xpipe.redis.core.server.FakeRedisServer;

/**
 * @author wenchao.meng
 *
 * Jan 6, 2017
 */
public class FakeRedis {

	public static void main(String[] args) throws Exception {
		
		int port = Integer.parseInt(System.getProperty("port", "6379"));
		
		FakeRedisServer fakeRedisServer = new FakeRedisServer(port);
		fakeRedisServer.setSendHalfRdbAndCloseConnectionCount(1);
		fakeRedisServer.initialize();
		fakeRedisServer.start();
	}

}
