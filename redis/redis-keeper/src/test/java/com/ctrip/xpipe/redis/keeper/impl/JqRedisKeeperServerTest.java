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
	public void startKeeper6000() throws Exception {

		startKeeper("keeper6000.xml", "jq");
	}

	@Test
	public void startKeeper6001() throws Exception {

		startKeeper("keeper6001.xml", "jq");
	}

	@After
	public void afterOneBoxTest() throws IOException {

		System.out.println("Press any key to exit");
		System.in.read();
	}

}
