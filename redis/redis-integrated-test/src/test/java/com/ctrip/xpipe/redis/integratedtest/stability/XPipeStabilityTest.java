package com.ctrip.xpipe.redis.integratedtest.stability;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.dianping.cat.Cat;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author shyin
 *
 *         Oct 9, 2016
 */
public class XPipeStabilityTest extends AbstractTest {
	private ExecutorService producerThreadPool;
	private ExecutorService consumerThreadPool;
	private ExecutorService valueCheckThreadPool;
	private ScheduledExecutorService expireCheckThreadPool;
	private ScheduledExecutorService qpsCheckThreadPool;
	private ScheduledExecutorService catLogMetricThreadPool;
	private JedisPool masterPool;
	private JedisPool slavePool;

	private ConcurrentHashMap<String, String> records = new ConcurrentHashMap<>(20000);
	private ConcurrentLinkedQueue<Pair<String, String>> valueCheckQueue = new ConcurrentLinkedQueue<>();

	private AtomicLong globalCnt = new AtomicLong(0);
	private AtomicLong queryCnt = new AtomicLong(0);
	private Long historyQueryCnt = new Long(0);
	private AtomicLong catIntervalCnt = new AtomicLong(0);
	private Long historyCatIntervalCnt = new Long(0);
	private AtomicLong catIntervalTotalDelay = new AtomicLong(0);
	private Long historyCatIntervalTotalDelay = new Long(0);

	public int TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("timeout", "10"));
	public int KEY_EXPIRE_SECONDS = Integer.parseInt(System.getProperty("key-expire-seconds", "500"));
	public int QPS_COUNT_INTERVAL = Integer.parseInt(System.getProperty("qps-count-interval", "5"));
	private int producerThreadNum = Integer.parseInt(System.getProperty("thread", "4"));
	private int msgSize = Integer.parseInt(System.getProperty("msg-size", "100"));
	private int catIntervalSize = Integer.parseInt(System.getProperty("cat-interval-size", "100"));

	private String masterAddress = System.getProperty("master", "127.0.0.1");
	private int masterPort = Integer.parseInt(System.getProperty("master-port", "6379"));
	private String slaveAddress = System.getProperty("slave", "127.0.0.1");
	private int slavePort = Integer.parseInt(System.getProperty("slave-port", "6379"));

	@Before
	public void setUp() {
		logger.info("[setUp]");
		Cat.initializeByDomain("100004376");

		producerThreadPool = Executors.newFixedThreadPool(producerThreadNum,
				XpipeThreadFactory.create("ProducerThreadPool"));
		consumerThreadPool = Executors.newFixedThreadPool(1, XpipeThreadFactory.create("ConsumerThreadPool"));
		valueCheckThreadPool = Executors.newFixedThreadPool(producerThreadNum,
				XpipeThreadFactory.create("ValueCheckThreadPool"));
		expireCheckThreadPool = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("ExpireCheckThreadPool"));
		qpsCheckThreadPool = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("QpsCheckThreadPool"));
		catLogMetricThreadPool = Executors.newScheduledThreadPool(1,
				XpipeThreadFactory.create("CatLogMetricThreadPool"));

		Thread.setDefaultUncaughtExceptionHandler(new XPipeStabilityTestExceptionHandler() {
			@Override
			protected void doRestart() {
				logger.info("[{}][No-Restart]", Thread.currentThread());
			}
		});

		masterPool = getJedisPool(masterAddress, masterPort, producerThreadNum * 2, 8);
		slavePool = getJedisPool(slaveAddress, slavePort, producerThreadNum * 3, 24);
	}

	@After
	public void tearDown() {
		logger.info("[tearDown]");
		producerThreadPool.shutdownNow();
		consumerThreadPool.shutdownNow();
		expireCheckThreadPool.shutdownNow();
		valueCheckThreadPool.shutdownNow();

		qpsCheckThreadPool.shutdownNow();
		catLogMetricThreadPool.shutdownNow();

		masterPool.destroy();
		slavePool.destroy();
	}

	@Test
	public void statbilityTest() {
		startConsumerJob();
		startProducerJob();
		startValueCheckJob();

		startQpsCheckJob();
		startCatLogMetricJob();
		startExpireCheckJob();

		try {
			TimeUnit.DAYS.sleep(7);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void startProducerJob() {
		for (int jobCnt = 0; jobCnt != producerThreadNum; ++jobCnt) {
			producerThreadPool.execute(new ProducerThread());
		}
	}

	private class ProducerThread implements Runnable {
		@SuppressWarnings({ "static-access" })
		@Override
		public void run() {
			Thread.currentThread().setDefaultUncaughtExceptionHandler(new XPipeStabilityTestExceptionHandler() {
				@Override
				protected void doRestart() {
					producerThreadPool.execute(new ProducerThread());
				}
			});

			Jedis master = null;
			try {
				master = masterPool.getResource();
				String key = null, value = null;
				while (true) {
					try {
						key = Long.toString(globalCnt.getAndIncrement()) + "-" + Long.toString(System.nanoTime());
						value = randomString(msgSize);
						records.put(key, value);
						master.setex(key, KEY_EXPIRE_SECONDS, value);
						queryCnt.incrementAndGet();

					} catch (JedisConnectionException e) {
						logger.error("[startProducerJob][run]JedisConnectionException : {}", e);
						records.remove(key);
						throw e;
					} catch (Exception e) {
						logger.error("[startProducerJob][run]InsertValue Exception : Key:{} Exception:{}", key, e);
						records.remove(key);
						throw e;
					}
				}
			} finally {
				if (null != master) {
					master.close();
				}
			}
		}
	}

	private void startConsumerJob() {
		consumerThreadPool.execute(new Runnable() {
			@SuppressWarnings("static-access")
			@Override
			public void run() {
				Thread.currentThread().setDefaultUncaughtExceptionHandler(new XPipeStabilityTestExceptionHandler() {
					@Override
					protected void doRestart() {
						startConsumerJob();
					}
				});

				Jedis slave = null;
				try {
					slave = slavePool.getResource();
					slave.psubscribe(new XPipeStabilityTestJedisPubSub(), "__key*__:*");

				} finally {
					if (null != slave) {
						slave.close();
					}
				}
			}
		});
	}

	private class XPipeStabilityTestJedisPubSub extends JedisPubSub {
		@Override
		public void onPMessage(String pattern, String channel, String msg) {
			String key = msg;
			String value = records.get(key);
			valueCheckQueue.offer(Pair.of(key, value));
			records.remove(key);

			catIntervalCnt.incrementAndGet();
			long delay = System.nanoTime() - Long.valueOf(key.substring(key.indexOf("-") + 1));
			catIntervalTotalDelay.set(catIntervalTotalDelay.get() + delay);
		}
	}

	private void startValueCheckJob() {
		for (int valueCheckThreadCnt = 0; valueCheckThreadCnt != producerThreadNum; ++valueCheckThreadCnt) {
			valueCheckThreadPool.execute(new ValueCheckThread());
		}
	}

	private class ValueCheckThread implements Runnable {
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
				while (true) {
					Pair<String, String> pair = valueCheckQueue.poll();
					if (null != pair) {
						try {
							String key = pair.getKey();
							String value = pair.getValue();
							String actualValue = slave.get(key);
							if (!value.equals(actualValue)) {
								logger.error("[startValueCheckJob][run][ValueCheck]Key:{}, Expect:{}, Get:{}", key,
										value, actualValue);
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

	private void startCatLogMetricJob() {
		catLogMetricThreadPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long catInterval = catIntervalCnt.get();
				long catIntervalDelay = catIntervalTotalDelay.get();
				long count = catInterval - historyCatIntervalCnt;
				if (count >= catIntervalSize) {
					long delay = catIntervalDelay - historyCatIntervalTotalDelay;
					historyCatIntervalCnt = catInterval;
					historyCatIntervalTotalDelay = catIntervalDelay;
					Cat.logMetricForSum("xpipe.redis.delay", delay / count);
				}
			}
		}, 0, 5, TimeUnit.MILLISECONDS);
	}

	private void startQpsCheckJob() {
		qpsCheckThreadPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long qps = (queryCnt.get() - historyQueryCnt) / QPS_COUNT_INTERVAL;
				historyQueryCnt = queryCnt.get();
				Cat.logMetricForSum("xpipe.redis.qps", qps);
				Cat.logMetricForSum("xpipe.redis.map", records.size());
				Cat.logMetricForSum("xpipe.redis.queue", valueCheckQueue.size());
				logger.info("[startQpsCheckJob][run]QPS : {}", qps);
				logger.info("[startQpsCheckJob][run]MapSize : {}", records.size());
				logger.info("[startQpsCheckJob][run]QueueSize : {}", valueCheckQueue.size());
			}
		}, 1, QPS_COUNT_INTERVAL, TimeUnit.SECONDS);
	}

	private void startExpireCheckJob() {
		expireCheckThreadPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				for (String key : records.keySet()) {
					long currentTime = System.nanoTime();
					long timeout = TimeUnit.NANOSECONDS
							.toSeconds(currentTime - Long.valueOf(key.substring(key.indexOf("-") + 1)));
					if (timeout > TIMEOUT_SECONDS) {
						if (null != records.get(key)) {
							logger.error("[startExpireCheckJob][run][Timeout]Key:{} Timeout:{}", key, timeout);
						}
					}
				}
			}
		}, 1, 1, TimeUnit.SECONDS);
	}

	private JedisPool getJedisPool(String ip, int port, int maxTotal, int maxIdle) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		return new JedisPool(config, ip, port, 0);
	}

	public abstract class XPipeStabilityTestExceptionHandler implements UncaughtExceptionHandler {
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			if (e instanceof InterruptedException) {
				logger.error("[XPipeStabilityTestExceptionHandler][InterruptedException][{}]", Thread.currentThread(),
						e);
			} else {
				logger.error("[XPipeStabilityTestExceptionHandler][UncaughtException][{}]", Thread.currentThread(), e);
				logger.info("[XPipeStabilityTestExceptionHandler][UncaughtException][{}]Try with doRestart",
						Thread.currentThread());
				doRestart();
			}
		}

		protected abstract void doRestart();
	}
}
