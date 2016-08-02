/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 5:27:17 PM
 */
public class JqRedisKeeperServerTest extends BaseRedisKeeperServerTest {
	
	@Override
	protected boolean deleteTestDir() {
		return false;
	}

	@Test
	public void startKeeper6666() throws Exception {

		startKeeper("keeper6666.xml", "jq");
	}

	@Test
	public void startKeeper7777() throws Exception {

		startKeeper("keeper7777.xml", "jq");
	}

	@After
	public void afterOneBoxTest() throws IOException {

		System.out.println("Press any key to exit");
		System.in.read();
	}

}
