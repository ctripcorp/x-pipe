package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author shyin
 *
 * Oct 14, 2016
 */
public class XPipeThreadPoolFactoryNamingTest extends AbstractTest{
	@Test
	public void XpipeThreadPoolFactoryNamingTest() {
		ExecutorService testThreadPool = Executors.newFixedThreadPool(1, XpipeThreadFactory.create("TestThreadPool"));
		testThreadPool.submit(new Runnable() {
			@Override
			public void run() {
				logger.info("[XpipeThreadPoolFactoryNamingTest]Test");
			}
		});
	}
}
