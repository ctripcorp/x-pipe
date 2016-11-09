package com.ctrip.xpipe.redis.integratedtest.stability;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map.Entry;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.dianping.cat.Cat;
import com.fasterxml.jackson.annotation.JsonIgnore;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author shyin
 *
 *         Oct 9, 2016
 */
public class XPipeStabilityTest {

	@JsonIgnore
	private Logger logger = LoggerFactory.getLogger(XPipeStabilityTest.class);

	@JsonIgnore
	private ScheduledExecutorService producerThreadPool;
	@JsonIgnore
	private ExecutorService consumerThreadPool;
	@JsonIgnore
	private ExecutorService valueCheckThreadPool;
	@JsonIgnore
	private ScheduledExecutorService expireCheckThreadPool;
	@JsonIgnore
	private ScheduledExecutorService qpsCheckThreadPool;
	@JsonIgnore
	private ScheduledExecutorService catLogMetricThreadPool;
	@JsonIgnore
	private JedisPool masterPool;
	@JsonIgnore
	private JedisPool slavePool;

	@JsonIgnore
	private ConcurrentHashMap<String, String> records = new ConcurrentHashMap<>(20000);
	@JsonIgnore
	private ConcurrentLinkedQueue<Pair<String, String>> valueCheckQueue = new ConcurrentLinkedQueue<>();

	private AtomicLong globalCnt = new AtomicLong(0);
	private AtomicLong queryCnt = new AtomicLong(0);
	private Long historyQueryCnt = new Long(0);
	private AtomicLong catIntervalCnt = new AtomicLong(0);
	private Long historyCatIntervalCnt = new Long(0);
	private AtomicLong catIntervalTotalDelay = new AtomicLong(0);
	private Long historyCatIntervalTotalDelay = new Long(0);

	public int MAX_KEY_COUNT = Integer.parseInt(System.getProperty("max-key-count", "20000000"));
	public int TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("timeout", "10"));
	public int KEY_EXPIRE_SECONDS = Integer.parseInt(System.getProperty("key-expire-seconds", "3600"));
	public int QPS_COUNT_INTERVAL = Integer.parseInt(System.getProperty("qps-count-interval", "5"));
	private int producerThreadNum = Integer.parseInt(System.getProperty("thread", "8"));
	private int valueCheckThreadNum = Integer.parseInt(System.getProperty("valueCheckThread", "8"));
	private int msgSize = Integer.parseInt(System.getProperty("msg-size", "100"));
	private int catIntervalSize = Integer.parseInt(System.getProperty("cat-interval-size", "100"));

	private String masterAddress = System.getProperty("master", "127.0.0.1");
	private int masterPort = Integer.parseInt(System.getProperty("master-port", "6379"));
	private String slaveAddress = System.getProperty("slave", "127.0.0.1");
	private int slavePort = Integer.parseInt(System.getProperty("slave-port", "6379"));

	@Before
	public void setUp() {
		logger.info("config:{}", new JsonCodec(false, true).encode(this));
		valueCheckThreadNum = (producerThreadNum < valueCheckThreadNum) ? valueCheckThreadNum : producerThreadNum;
		logger.info("[ProducerThread]{} [ValueCheckThread]{}", producerThreadNum, valueCheckThreadNum);
		logger.info("[KeyExpireSeconds]{}", KEY_EXPIRE_SECONDS);

		producerThreadPool = Executors.newScheduledThreadPool(producerThreadNum,
				XpipeThreadFactory.create("ProducerThreadPool"));
		consumerThreadPool = Executors.newFixedThreadPool(1, XpipeThreadFactory.create("ConsumerThreadPool"));
		valueCheckThreadPool = Executors.newFixedThreadPool(valueCheckThreadNum,
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

		masterPool = getJedisPool(masterAddress, masterPort, producerThreadNum * 2, producerThreadNum, 2000);
		slavePool = getJedisPool(slaveAddress, slavePort, valueCheckThreadNum * 2, valueCheckThreadNum, 2000);
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
			producerThreadPool.scheduleAtFixedRate(new ProducerThread(), 0, 1, TimeUnit.MILLISECONDS);
		}
	}

	private class ProducerThread implements Runnable {
		@Override
		public void run() {
			Jedis master = null;
			String key = null, value = null;
			try {
				master = masterPool.getResource();
				key = Long.toString(globalCnt.getAndIncrement() % MAX_KEY_COUNT);
				String nanoTime = Long.toString(System.nanoTime());
				String currentTime = Long.toString(System.currentTimeMillis());
				value = buildValue(msgSize, nanoTime, "-", currentTime, "-");

				records.put(key, value);
				master.setex(key, KEY_EXPIRE_SECONDS, value);
				queryCnt.incrementAndGet();
			} catch (JedisException e) {
				logger.error("[startProducerJob][run]JedisException : {}", e);
				records.remove(key);
			} catch (Exception e) {
				logger.error("[startProducerJob][run]InsertValue Exception : Key:{} Value:{} Exception:{}", key, value,
						e);
				records.remove(key);
			} finally {
				if (null != master) {
					master.close();
				}
			}
		}
	}

	private String buildValue(int size, String... strings) {
		StringBuilder sb = new StringBuilder();
		for (String str : strings) {
			sb.append(str);
		}
		if (sb.length() < size) {
			sb.append(randomString(size - sb.length()));
		}
		return sb.toString();
	}

	private void startConsumerJob() {
		consumerThreadPool.execute(new Runnable() {
			@SuppressWarnings("static-access")
			@Override
			public void run() {
				Thread.currentThread().setDefaultUncaughtExceptionHandler(new XPipeStabilityTestExceptionHandler() {
					@Override
					protected void doRestart() {
						logger.info("[doRestart]Consumer Job");
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
			if (null != value) {
				valueCheckQueue.offer(Pair.of(key, value));
				records.remove(key);

				catIntervalCnt.incrementAndGet();
				long delay = System.nanoTime() - Long.valueOf(value.substring(0, value.indexOf("-")));
				catIntervalTotalDelay.set(catIntervalTotalDelay.get() + delay);
			} else {
				logger.error("[XPipeStabilityTestJedisPubSub][Get Null From records]Key:{} Value:{}", key, value);
			}
		}
	}

	private void startValueCheckJob() {
		for (int valueCheckThreadCnt = 0; valueCheckThreadCnt != valueCheckThreadNum; ++valueCheckThreadCnt) {
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
			
			private long previousReceiveCnt = 0;
			private long previousReceiveDelay = 0;
			
			@Override
			public void run() {
				
				long qps = (queryCnt.get() - historyQueryCnt) / QPS_COUNT_INTERVAL;
				historyQueryCnt = queryCnt.get();
				
				long currentReceiveCount = catIntervalCnt.get();
				long currentReceiveDelay = catIntervalTotalDelay.get();
				
				long averageDelay = 0;
				if((currentReceiveCount - previousReceiveCnt) > 0){
					averageDelay = (currentReceiveDelay - previousReceiveDelay)/(currentReceiveCount - previousReceiveCnt);
					previousReceiveCnt = currentReceiveCount;
					previousReceiveDelay = currentReceiveDelay;
				}
				
				
				Cat.logMetricForSum("xpipe.redis.qps", qps);
				Cat.logMetricForSum("xpipe.redis.map", records.size());
				Cat.logMetricForSum("xpipe.redis.queue", valueCheckQueue.size());
				
				logger.info("[startQpsCheckJob][run]QPS : {}", qps);
				logger.info("[startQpsCheckJob][run][delay] : {} micro seconds", averageDelay/(1000));
				logger.info("[startQpsCheckJob][run]MapSize : {}", records.size());
				logger.info("[startQpsCheckJob][run]QueueSize : {}", valueCheckQueue.size());
			}
		}, 1, QPS_COUNT_INTERVAL, TimeUnit.SECONDS);
	}

	private void startExpireCheckJob() {
		
		expireCheckThreadPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				
				for (Entry<String, String> entry : records.entrySet()) {
					
					String key = entry.getKey();
					String value = entry.getValue();
					
					long currentTime = System.nanoTime();
					long timeout = TimeUnit.NANOSECONDS
							.toSeconds(currentTime - Long.valueOf(value.substring(value.indexOf("-") + 1)));
					if (timeout > TIMEOUT_SECONDS) {
						if (null != records.get(key)) {
							logger.error("[startExpireCheckJob][run][Timeout]Key:{} Timeout:{}", key, timeout);
						}
					}
					if (timeout > 3 * TIMEOUT_SECONDS) {
						if (null != records.get(key)) {
							logger.error("[startExpireCheckJob][run][Timeout][NoMoreCheck]Key:{}", key, timeout);
							records.remove(key);
						}
					}
				}
			}
		}, 1, 1, TimeUnit.SECONDS);
	}

	private JedisPool getJedisPool(String ip, int port, int maxTotal, int maxIdle, int timeout) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		return new JedisPool(config, ip, port, timeout);
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

	public static String randomString() {

		return randomString(1 << 10);
	}

	public static String randomString(int length) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++) {
			sb.append((char) ('a' + (int) (26 * Math.random())));
		}

		return sb.toString();

	}
}
