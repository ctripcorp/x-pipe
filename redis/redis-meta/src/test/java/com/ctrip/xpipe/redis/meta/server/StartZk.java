/**
 * 
 */
package com.ctrip.xpipe.redis.meta.server;


import org.apache.curator.test.TestingServer;
import org.junit.Test;

/**
 * @author marsqing
 *
 *         Jun 15, 2016 10:25:43 AM
 */
public class StartZk {

	@Test
	public void start() throws Exception {
		startZk();

		System.out.println("Press any key to exit...");
		System.in.read();
	}

	@SuppressWarnings("resource")
	private void startZk() {
		try {
			new TestingServer(2181).start();
		} catch (Exception e) {
		}
	}

}
