package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 *         Oct 19, 2016
 */
public class MasterSlaveTest extends AbstractRedisTest{

	private String masterAddr = System.getProperty("master", "localhost:6379");
	private String slaveAddr = System.getProperty("slave", "localhost:6479");
	private int sleep = Integer.parseInt(System.getProperty("sleep", "1"));
	private int messageSize =  Integer.parseInt(System.getProperty("messageSize", String.valueOf(25 * 1024)));

	private Jedis master, slave;
	private AtomicLong count = new AtomicLong();

	@Before
	public void beforeMasterSlaveTest() {
		master = createJedis(masterAddr);
		slave = createJedis(slaveAddr);
		logger.info("{}, {}, {}, messageSize:{}", masterAddr, slaveAddr, sleep, messageSize);
		
	}

	public void setValue() {

		final int countEach = 200 * (1 << 10);
		int concurrent = 20;
		final String value = randomString(1 << 10);

		for (int i = 0; i < concurrent; i++) {
			executors.execute(new Runnable() {

				@Override
				public void run() {

					Jedis master = createJedis(masterAddr);
					for (int i = 0; i < countEach; i++) {
						master.setex(getKey(), 24*3600, value);
					}

				}
			});
		}
	}

	@Test
	public void testDelay() throws IOException {
		
		scheduled.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {

				String key = getKey();
				String value = randomString(messageSize);
				try {
					master.set(key, value);
					sleep(sleep);
					String slaveValue = slave.get(key);
					if (!value.equals(slaveValue)) {
						logger.error("[master!=slave]{}, {},{}", key, value.substring(0, Math.min(40, value.length())), slaveValue);
						return;
					}
					logger.info("[success]{}", key);
				} catch (Throwable th) {
					logger.error("key:" + key, th);
				}
			}
		}, 0, 1, TimeUnit.SECONDS);

	}

	protected String getKey() {
		return count.incrementAndGet() + "," + System.currentTimeMillis();
	}

	@After
	public void afterMasterSlaveTest() throws IOException {
		waitForAnyKeyToExit();
	}
}
