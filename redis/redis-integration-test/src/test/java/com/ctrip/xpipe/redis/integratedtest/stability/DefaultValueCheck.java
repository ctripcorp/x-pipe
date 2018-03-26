package com.ctrip.xpipe.redis.integratedtest.stability;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.integratedtest.stability.XPipeStabilityTest.XPipeStabilityTestExceptionHandler;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.tuple.Pair;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Nov 14, 2016
 */
public class DefaultValueCheck extends AbstractStartStoppable implements ValueCheck{

	private int valueCheckThreadNum;

	private MetricLog metricLog;

	@JsonIgnore
	private ExecutorService valueCheckThreadPool;

	private JedisPool slavePool;

	private AtomicLong offerCount = new AtomicLong();

	@JsonIgnore
	private ConcurrentLinkedQueue<Pair<String, String>> valueCheckQueue = new ConcurrentLinkedQueue<>();

	public DefaultValueCheck(int valueCheckThreadNum, JedisPool slavePool, MetricLog metricLog){
		this.valueCheckThreadNum = valueCheckThreadNum;
		this.slavePool = slavePool;
		this.metricLog = metricLog;
	}

	@Override
	public void offer(Pair<String, String> checkData){

		offerCount.incrementAndGet();
		valueCheckQueue.offer(checkData);
	}

	@Override
	public long queueSize(){

		return valueCheckQueue.size();
	}

	@Override
	protected void doStart() throws Exception {

		valueCheckThreadPool = Executors.newFixedThreadPool(valueCheckThreadNum,
				XpipeThreadFactory.create("ValueCheckThreadPool"));

		for (int valueCheckThreadCnt = 0; valueCheckThreadCnt != valueCheckThreadNum; ++valueCheckThreadCnt) {
			valueCheckThreadPool.execute(new ValueCheckThread());
		}
	}

	@Override
	protected void doStop() {
		valueCheckThreadPool.shutdownNow();

	}

	private class ValueCheckThread implements Runnable {

	    private String slaveDesc = null;

		@SuppressWarnings({ "static-access" })
		@Override
		public void run() {
			Thread.currentThread().setDefaultUncaughtExceptionHandler(new XPipeStabilityTestExceptionHandler() {
				@Override
				protected void doRestart() {
					valueCheckThreadPool.execute(new ValueCheckThread());
				}
			});

			Jedis slave = null;
			try {
				slave = slavePool.getResource();
				if(slaveDesc == null){
				    slaveDesc = String.format("%s.%d", slave.getClient().getHost(), slave.getClient().getPort());
                }
				while (true) {
					Pair<String, String> pair = valueCheckQueue.poll();
					if (null != pair) {
						try {
							String key = pair.getKey();
							String pre = pair.getValue();

							String actualValue = slave.get(key);
							String actualCompare = actualValue.substring(0, pre.length());
							if (!pre.equals(actualCompare)) {
								logger.error("[startValueCheckJob][run][ValueCheck]Key:{}, Expect:{}, Get:{}", key,
										pre, actualCompare);
                                metricLog.log("notice.notequal", slaveDesc,1);
							}
						} catch (JedisConnectionException e) {
							logger.error("[startValueCheckJob][run]JedisConnectionException:{}", e);
							valueCheckQueue.offer(pair);
							throw e;
						}
					}
				}
			} finally {
				if (null != slave) {
					slave.close();
				}
			}
		}
	}
}
