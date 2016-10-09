package com.ctrip.xpipe.redis.integratedtest.function.stress;

import redis.clients.jedis.Jedis;

/**
 * @author liu
 *
 * Oct 9, 2016
 */
public class StressTestBySet extends AbstractStress{

	public StressTestBySet(long testCount, long threadNum,
			int pageSizeInOneMsSleep) {
		super(testCount, threadNum, pageSizeInOneMsSleep);
	}
	public StressTestBySet() {
		super();
	}

	@Override
	protected String getOnPMessageChannel() {
		return "*";
	}

	@Override
	protected void operation(Jedis master, String key, String value) {
		master.set(key, value);		
	}
	public static void main(String[] args) {
		StressTestBySet test=new StressTestBySet();
		test.startTest();
	}
}
