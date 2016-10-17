package com.ctrip.xpipe.redis.integratedtest.function.stress;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import redis.clients.jedis.Jedis;

/**
 * @author liu
 * 
 *         Oct 9, 2016
 */
@RunWith(Parameterized.class)
public class StressTestByPublish extends AbstractStress {

	public StressTestByPublish(long testCount, long threadNum,
			int numberPerMillisecond, String masterIp, int masterPort,
			String slaveIp, int slavePort, int valueLength) {
		super(testCount, threadNum, numberPerMillisecond, masterIp, masterPort,
				slaveIp, slavePort, valueLength);
	}

	@Parameters
	public static Collection prepareData() {
		Object[][] object = {
				{ 100, 2, 5, "0.0.0.127", 6379, "0.0.0.127", 6379, 20 },
				{ 100, 2, 5, "0.0.0.127", 6379, "0.0.0.127", 6379, 20 } };
		return Arrays.asList(object);
	}

	@Test
	public void startStressTest() {
		this.startTest();
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		super.uncaughtException(t, e);
	}

	@Override
	protected int getMasterMaxTotal() {
		return 40;
	}

	@Override
	protected int getMasterMaxIdle() {
		return 5;
	}

	@Override
	protected int getSlaveMaxTotal() {
		return 40;
	}

	@Override
	protected int getSlaveMaxIdle() {
		return 5;
	}

	@Override
	protected String getChannel() {
		return UUID.randomUUID().toString();
	}

	@Override
	protected void operation(Jedis master, String key, String value) {
		master.publish(channel, key);
	}
}
