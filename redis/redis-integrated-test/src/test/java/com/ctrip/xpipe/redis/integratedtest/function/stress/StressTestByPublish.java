package com.ctrip.xpipe.redis.integratedtest.function.stress;

import redis.clients.jedis.Jedis;

/**
 * @author liu
 * 
 *         Oct 9, 2016
 */
public class StressTestByPublish extends AbstractStress {

	public StressTestByPublish(long testCount, long threadNum,
			int pageSizeInOneMsSleep) {
		super(testCount, threadNum, pageSizeInOneMsSleep);
	}

	public StressTestByPublish() {
		super();
	}

	@Override
	protected String getOnPMessageChannel() {
		return super.channel;
	}

	@Override
	protected void operation(Jedis master, String key, String value) {
		master.publish(super.channel, key);
	}

	public static void main(String[] args) {
		StressTestByPublish test = new StressTestByPublish();
		test.startTest();
	}

}
