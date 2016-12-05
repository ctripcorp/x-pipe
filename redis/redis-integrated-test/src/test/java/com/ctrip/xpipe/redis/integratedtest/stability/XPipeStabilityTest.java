package com.ctrip.xpipe.redis.integratedtest.stability;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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

	private AtomicLong globalCnt = new AtomicLong(0);
	private AtomicLong queryCnt = new AtomicLong(0);
	private Long historyQueryCnt = new Long(0);
	private AtomicLong catIntervalCnt = new AtomicLong(0);
	private Long historyCatIntervalCnt = new Long(0);
	private AtomicLong catIntervalTotalDelay = new AtomicLong(0);
	private Long historyCatIntervalTotalDelay = new Long(0);

	public int runDays = Integer.parseInt(System.getProperty("run-days", "365"));


	public boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "true"));
	
	public int MAX_KEY_COUNT = Integer.parseInt(System.getProperty("max-key-count", "20000000"));
	public int TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("timeout", "10"));
	public int KEY_EXPIRE_SECONDS = Integer.parseInt(System.getProperty("key-expire-seconds", "3600"));
	public int QPS_COUNT_INTERVAL = Integer.parseInt(System.getProperty("qps-count-interval", "5"));
	private int producerThreadNum = Integer.parseInt(System.getProperty("thread", "8"));
	private int producerIntervalMicro = Integer.parseInt(System.getProperty("producer-interval-micro", "1000"));

	private boolean startValueCheck = Boolean.parseBoolean(System.getProperty("start-value-check", "true"));
	private int valueCheckThreadNum = Integer.parseInt(System.getProperty("valueCheckThread", "8"));
	private int msgSize = Integer.parseInt(System.getProperty("msg-size", "1000"));
	private int catIntervalSize = Integer.parseInt(System.getProperty("cat-interval-size", "100"));
	
	private String clusterName = System.getProperty("cluster-name", "xpipe");
	private String shardName = System.getProperty("shard-name", "xpipe");

	private String masterAddress = System.getProperty("master", "127.0.0.1");
	private String randomStr = null;
	private int masterPort = Integer.parseInt(System.getProperty("master-port", "6379"));
	private String slaveAddress = System.getProperty("slave", "127.0.0.1");
	private int slavePort = Integer.parseInt(System.getProperty("slave-port", "6379"));

	private ValueCheck valueCheck;

	@Before
	public void setUp() {

		logger.info("config:{}", new JsonCodec(false, true).encode(this));
		valueCheckThreadNum = (producerThreadNum < valueCheckThreadNum) ? valueCheckThreadNum : producerThreadNum;
		logger.info("[ProducerThread]{} [ValueCheckThread]{}", producerThreadNum, valueCheckThreadNum);
		logger.info("[KeyExpireSeconds]{}", KEY_EXPIRE_SECONDS);

		producerThreadPool = Executors.newScheduledThreadPool(producerThreadNum,
				XpipeThreadFactory.create("ProducerThreadPool"));
		consumerThreadPool = Executors.newFixedThreadPool(1, XpipeThreadFactory.create("ConsumerThreadPool"));
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

		randomStr = randomString(msgSize);

		masterPool = getJedisPool(masterAddress, masterPort, producerThreadNum * 2, producerThreadNum, 2000);
		slavePool = getJedisPool(slaveAddress, slavePort, valueCheckThreadNum * 2, valueCheckThreadNum, 2000);
		
		if(startValueCheck){
			
			logger.info("[setUp][addValueCheck]");
			valueCheck = new DefaultValueCheck(valueCheckThreadNum, slavePool);
		}else{
			
			logger.info("[setUp][NullValueCheck]");
			valueCheck = new NullValueCheck();
		}
	}

	@After
	public void tearDown() throws Exception {

		logger.info("[tearDown]");
		producerThreadPool.shutdownNow();
		consumerThreadPool.shutdownNow();
		expireCheckThreadPool.shutdownNow();

		valueCheck.stop();
		qpsCheckThreadPool.shutdownNow();
		catLogMetricThreadPool.shutdownNow();

		masterPool.destroy();
		slavePool.destroy();
	}

	@Test
	public void statbilityTest() throws Exception {

		valueCheck.start();
		startConsumerJob();
		startProducerJob();

		startQpsCheckJob();
		startCatLogMetricJob();
		startExpireCheckJob();

		TimeUnit.DAYS.sleep(runDays);
	}

	private void startProducerJob() {
		for (int jobCnt = 0; jobCnt != producerThreadNum; ++jobCnt) {
			producerThreadPool.scheduleAtFixedRate(new ProducerThread(), 0, producerIntervalMicro,
					TimeUnit.MICROSECONDS);
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
		sb.append(randomStr);
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
				valueCheck.offer(Pair.of(key, value));
				records.remove(key);

				catIntervalCnt.incrementAndGet();
				long delay = System.nanoTime() - Long.valueOf(value.substring(0, value.indexOf("-")));
				catIntervalTotalDelay.set(catIntervalTotalDelay.get() + delay);
			} else {
				logger.error("[XPipeStabilityTestJedisPubSub][Get Null From records]Key:{} Value:{}", key, value);
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
					Cat.logMetricForSum(formatCatLogTitle("xpipe.redis.delay", clusterName, shardName), delay / count);
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
				if ((currentReceiveCount - previousReceiveCnt) > 0) {
					averageDelay = (currentReceiveDelay - previousReceiveDelay)
							/ (currentReceiveCount - previousReceiveCnt);
					previousReceiveCnt = currentReceiveCount;
					previousReceiveDelay = currentReceiveDelay;
				}

				Cat.logMetricForSum(formatCatLogTitle("xpipe.redis.qps", clusterName, shardName), qps);
				if(DEBUG) {
					Cat.logMetricForSum(formatCatLogTitle("xpipe.redis.map", clusterName, shardName), records.size());
					Cat.logMetricForSum(formatCatLogTitle("xpipe.redis.queue", clusterName, shardName), valueCheck.queueSize());
				}

				logger.info("[startQpsCheckJob][run]QPS : {}", qps);
				logger.info("[startQpsCheckJob][run][delay] : {} micro seconds", averageDelay / (1000));
				if(DEBUG) {
					logger.info("[startQpsCheckJob][run]MapSize : {}", records.size());
					logger.info("[startQpsCheckJob][run]QueueSize : {}", valueCheck.queueSize());
				}
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

	public static abstract class XPipeStabilityTestExceptionHandler implements UncaughtExceptionHandler {

		private Logger logger = LoggerFactory.getLogger(getClass());

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
	
	private String formatCatLogTitle(String prefix,String clusterName,String shardName) {
		return String.format("%s-$s-$s", prefix, clusterName, shardName);
	}
	
}
