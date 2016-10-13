package com.ctrip.xpipe.redis.integratedtest.function.stress;

import java.util.UUID;

import redis.clients.jedis.Jedis;

/**
 * @author liu
 * 
 *         Oct 9, 2016
 */
public class StressTestByPublish extends AbstractStress {

	@Override
	protected void operation(Jedis master, String key, String value) {
		master.publish(channel, key);
	}

	@Override
	protected String getChannel() {
		return channel;
	}

	@Override
	protected void setChannel() {
		channel=UUID.randomUUID().toString();
	}
	
	/**
	 * @param args
	 *  Stress tests
	 */
	public static void main(String[] args) {
		StressTestByPublish test = new StressTestByPublish();
		test.startTest();
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		super.uncaughtException(t, e);
	}

}
