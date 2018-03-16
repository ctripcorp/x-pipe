package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import com.ctrip.xpipe.redis.integratedtest.stability.DelayManager;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author wenchao.meng
 *
 *         Feb 14, 2017
 */
public class MultiThreadDelayTest extends AbstractIntegratedTest {

	private int threadCount = 10;

	private DelayManager delayManager;
	
	@Before
	public void beforeMultiThreadDelayTesr(){
		 delayManager = new DelayManager(scheduled, "delay", getTestName(), 10);
	}

	@Test
	public void testDelay() throws IOException {

		for (int i = 0; i < threadCount; i++) {

			executors.execute(new Runnable() {

				@Override
				public void run() {

					@SuppressWarnings("unused")
					int i = 0;
					while (true) {
						long nano = System.nanoTime();
						i++;
						delayManager.delay(System.nanoTime() - nano);
						sleep(1);
					}
				}
			});
		}
		
		waitForAnyKeyToExit();
	}

	@Override
	protected String getRedisTemplate() {
		return null;
	}

	@Override
	protected List<RedisMeta> getRedisSlaves() {
		return null;
	}

}
