package com.ctrip.xpipe.pool;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;

/**
 * @author wenchao.meng
 *
 *         Nov 8, 2016
 */
public class XpipeNettyClientObjectPoolTest extends AbstractTest {

	@Test
	//try xmx32m and run
	public void testGc() throws Exception {

		while (true) {

			XpipeNettyClientObjectPool pool = new XpipeNettyClientObjectPool();
			LifecycleHelper.initializeIfPossible(pool);
			LifecycleHelper.startIfPossible(pool);
			LifecycleHelper.stopIfPossible(pool);
			LifecycleHelper.disposeIfPossible(pool);
			sleep(10);
		}

	}

}
