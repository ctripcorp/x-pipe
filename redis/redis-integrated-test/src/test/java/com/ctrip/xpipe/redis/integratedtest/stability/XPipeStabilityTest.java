package com.ctrip.xpipe.redis.integratedtest.stability;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.ctrip.xpipe.AbstractTest;
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
	private ExecutorService expireCheckThreadPool;
	private ExecutorService valueCheckThreadPool;
	private ScheduledExecutorService qpsCheckThreadPool;
	private JedisPool masterPool;
	private JedisPool slavePool;

	private ConcurrentHashMap<String, String> records = new ConcurrentHashMap<>(20000);
	private ConcurrentLinkedQueue<Pair<String, String>> valueCheckQueue = new ConcurrentLinkedQueue<>();

	private AtomicInteger queryCount = new AtomicInteger(0);
	private AtomicLong globalCnt = new AtomicLong(0);

	public static int MILLIS_TO_NANOTIME = 1000000;
	public static int SECONDS_TO_MILLIS = 1000;

	public int TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("timeout", "10"));
	public int KEY_EXPIRE_SECONDS = Integer.parseInt(System.getProperty("key-expire-seconds", "500"));
	public int QPS_COUNT_INTERVAL = Integer.parseInt(System.getProperty("qps-count-interval","10"));
	private int producerThreadNum = Integer.parseInt(System.getProperty("thread", "4"));
	private int msgSize = Integer.parseInt(System.getProperty("msgSize", "100"));
	private String masterAddress = System.getProperty("master","127.0.0.1");
	private int masterPort = Integer.parseInt(System.getProperty("master-port","6379"));
	private String slaveAddress = System.getProperty("slave","127.0.0.1");
	private int slavePort = Integer.parseInt(System.getProperty("master-port","6379"));
	
	@Before
	public void setUp() {
		Cat.initializeByDomain("100004376");

		producerThreadPool = Executors.newFixedThreadPool(producerThreadNum);
		consumerThreadPool = Executors.newFixedThreadPool(1);
		expireCheckThreadPool = Executors.newFixedThreadPool(1);
		valueCheckThreadPool = Executors.newFixedThreadPool(producerThreadNum);
		qpsCheckThreadPool = Executors.newScheduledThreadPool(1);

		masterPool = getJedisPool(masterAddress, masterPort, producerThreadNum * 2, 1);
		slavePool = getJedisPool(slaveAddress, slavePort, producerThreadNum * 4 , 1);
	}

	@After
	public void tearDown() {
		producerThreadPool.shutdownNow();
		consumerThreadPool.shutdownNow();
		expireCheckThreadPool.shutdownNow();
		valueCheckThreadPool.shutdownNow();

		masterPool.destroy();
		slavePool.destroy();
	}

	@Test
	public void statbilityTest() throws IOException {
		startConsumerJob();
		startProducerJob();
		startQpsCheckJob();

		startExpireCheckJob();
		startValueCheckJob();

		waitForAnyKeyToExit();
	}

	private void startProducerJob() {
		for (int jobCnt = 0; jobCnt != producerThreadNum; ++jobCnt) {
			producerThreadPool.submit(new Runnable() {
				@SuppressWarnings("resource")
				@Override
				public void run() {
					Jedis master = masterPool.getResource();
					String key = null, value = null;
					while (true) {
						try {
							Cat.logEvent("xpipe.redis.test", "setCount");

							key = Long.toString(globalCnt.getAndIncrement()) + "-" + Long.toString(System.nanoTime());
							value = randomString(msgSize);
							records.put(key, value);

							master.setex(key, KEY_EXPIRE_SECONDS, value);
							logger.debug("[startProducerJob][run]Insert: <{}>", key);

							queryCount.incrementAndGet();
						} catch (JedisConnectionException e) {
							logger.error("[startProducerJob][run]JedisConnectionException : {}", e);
							records.remove(key);
							master = reconnectJedis(master, masterPool);
						} catch (Exception e) {
							logger.error("[startProducerJob][run]InsertValueException : Key:{} Exception:{}", key, e);
							records.remove(key);
						}
					}
				}
			});
		}
	}

	private void startConsumerJob() {
		consumerThreadPool.submit(new Runnable() {
			@SuppressWarnings("resource")
			@Override
			public void run() {
				Jedis slave = slavePool.getResource();
				slave.psubscribe(new XPipeStabilityTestJedisPubSub(), "__key*__:*");

				while (true) {
					if (!slave.isConnected()) {
						logger.error("[startConsumerJob][run]Consumer lose slave connection.");
						slave = reconnectJedis(slave, slavePool);
						slave.psubscribe(new XPipeStabilityTestJedisPubSub(), "__key*__:*");
					}
					sleep(1000);
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
			logger.debug("[XPipeStabilityTestJedisPubSub][onPMessage]Remove: <{}>", key);
			
			String nanotime_key = key.substring(key.indexOf("-") + 1);
			long delay = System.nanoTime() - Long.valueOf(nanotime_key);
			Cat.logEvent("xpipe.redis.test", "getCount");
			Cat.logMetricForSum("xpipe.redis.delay", delay);
		}
	}

	private void startQpsCheckJob() {
		qpsCheckThreadPool.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				logger.info("[startQpsCheckJob][run]QPS : {}", queryCount.get() / QPS_COUNT_INTERVAL);
				logger.info("[startQpsCheckJob][run]MapSize : {}", records.size());
				logger.info("[startQpsCheckJob][run]QueueSize : {}", valueCheckQueue.size());
				queryCount.set(0);
			}
		}, 1, QPS_COUNT_INTERVAL, TimeUnit.SECONDS);
	}

	private void startExpireCheckJob() {
		expireCheckThreadPool.submit(new Runnable() {
			@Override
			public void run() {
				while (true) {
					for (String key : records.keySet()) {
						long currentTime = System.nanoTime();
						String nanotime_key = key.substring(key.indexOf("-") + 1);
						long timeout = (currentTime - Long.valueOf(nanotime_key)) / MILLIS_TO_NANOTIME;
						if (timeout > TIMEOUT_SECONDS * SECONDS_TO_MILLIS) {
							if (null != records.get(key)) {
								logger.error("[startExpireCheckJob][run][Timeout]Key:{} Timeout:{}", key, timeout);
							}
						}
						sleep(1000);
					}
				}
			}
		});
	}

	private void startValueCheckJob() {
		for (int valueCheckThreadCnt = 0; valueCheckThreadCnt != producerThreadNum * 3; ++valueCheckThreadCnt) {
			valueCheckThreadPool.submit(new Runnable() {
				@SuppressWarnings("resource")
				@Override
				public void run() {
					Jedis slave = slavePool.getResource();
					while (true) {
						Pair<String, String> pair = valueCheckQueue.poll();
						if (null != pair) {
							try {
								String key = pair.getKey();
								String value = pair.getValue();
								String actualValue = slave.get(key);
								if (!actualValue.equals(value)) {
									logger.error("[startValueCheckJob][run][ValueCheck]Key:{}, Expect:{}, Get:{}", key,
											value, actualValue);
								}
							} catch (JedisConnectionException e) {
								logger.error("[startValueCheckJob][run]JedisConnectionException:{}", e);
								slave = reconnectJedis(slave, slavePool);
								valueCheckQueue.offer(pair);
							} catch (Exception e) {
								logger.error("[startValueCheckJob][run]Exception:{}, pair : {}", e, pair);
							}
						}
					}
				}
			});
		}
	}

	private JedisPool getJedisPool(String ip, int port, int maxTotal, int maxIdle) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		return new JedisPool(config, ip, port,0);
	}

	private Jedis reconnectJedis(Jedis jedis, JedisPool pool) {
		if (null != jedis) {
			jedis.close();
			return pool.getResource();
		}
		return pool.getResource();
	}
}
