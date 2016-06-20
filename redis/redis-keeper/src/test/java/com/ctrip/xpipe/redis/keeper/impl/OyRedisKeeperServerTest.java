/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.foundation.IdcUtil;


/**
 * @author marsqing
 *
 *         Jun 16, 2016 5:27:17 PM
 */
public class OyRedisKeeperServerTest extends BaseRedisKeeperServerTest {

	@Override
	protected void doIdcInit() {
		IdcUtil.setToOY();
	}

	@Test
	public void startKeeper4444() throws Exception {

		startKeeper("keeper4444.xml", "oy");
	}

	@Test
	public void startKeeper5555() throws Exception {

		startKeeper("keeper5555.xml", "oy");
	}

	@After
	public void afterOneBoxTest() throws IOException {

		System.out.println("Press any key to exit");
		System.in.read();
	}

}
