package com.ctrip.xpipe.redis.integratedtest.function.stress;

import redis.clients.jedis.Jedis;

/**
 * @author liu
 * 
 *         Oct 9, 2016
 */
public class StressTestBySet extends AbstractStress {

	@Override
	protected void operation(Jedis master, String key, String value) {
		master.set(key, value);
	}

	@Override
	protected String getChannel() {
		return channel;
	}

	@Override
	protected void setChannel() {
		channel = "*";
	}
	
	/**
	 * @param args
	 *  Stress tests
	 */
	public static void main(String[] args) {
		StressTestBySet test = new StressTestBySet();
		test.startTest();
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		super.uncaughtException(t, e);
	}
}
