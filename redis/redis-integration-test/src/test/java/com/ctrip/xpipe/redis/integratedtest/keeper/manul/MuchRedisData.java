package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedSingleDc;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.apache.commons.exec.ExecuteException;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wenchao.meng
 *
 *         Sep 29, 2016
 */
public class MuchRedisData extends AbstractKeeperIntegratedSingleDc {

	private int messageCount = 1 << 21;
	private int messageSize = 1 << 10;
	private int concurrentCount = 20;

	@Test
	public void startTest() throws IOException {

		waitForAnyKeyToExit();
	}

	@Override
	protected void startRedises() throws ExecuteException, IOException {
		super.startRedises();

		sendDataToMaster();
	}

	private void sendDataToMaster() {

		
		final CountDownLatch latch = new CountDownLatch(concurrentCount);

		logger.info("[sendDataToMaster][begin]{}, {}", messageCount, messageSize);
		for (int i = 0; i < concurrentCount; i++) {

			executors.execute(new AbstractExceptionLogTask() {

				@Override
				protected void doRun() throws Exception {
					try {
						sendMessage(getRedisMaster(), messageCount / concurrentCount, randomString(messageSize));
					} finally {
						latch.countDown();
					}
				}
			});
		}
		
		try {
			latch.await();
			logger.info("[sendDataToMaster][ end ]{}, {}", messageCount, messageSize);
		} catch (InterruptedException e) {
			logger.error("[sendDataToMaster]", e);
		}
	}
	
	@Override
	protected KeeperConfig getKeeperConfig() {
		return new TestKeeperConfig(1 << 30, 5, 1 << 30, 300000);
	}

}