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
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.dianping.cat.Cat;
import com.fasterxml.jackson.annotation.JsonIgnore;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

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

	public long MAX_MEMORY = Long.parseLong(System.getProperty("max-memory", String.valueOf( 10L * (1 << 30))));
	private long maxKeys;
	
	public int TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("timeout", "10"));
	public int TIME_TOO_LONG_TO_LOG_MILLI = Integer.parseInt(System.getProperty("time-long-log", "10"));
	public int QPS_COUNT_INTERVAL = Integer.parseInt(System.getProperty("qps-count-interval", "5"));
	private int producerThreadNum = Integer.parseInt(System.getProperty("thread", "8"));
	private int producerIntervalMicro = Integer.parseInt(System.getProperty("producer-interval-micro", "1000"));

	private boolean startValueCheck = Boolean.parseBoolean(System.getProperty("start-value-check", "true"));
	private int valueCheckThreadNum = Integer.parseInt(System.getProperty("valueCheckThread", "8"));
	private int msgSize = Integer.parseInt(System.getProperty("msg-size", "1000"));
	private int catIntervalSize = Integer.parseInt(System.getProperty("cat-interval-size", "100"));

	private String masterAddress = System.getProperty("master", "127.0.0.1");
	private ObjectPool<byte[]> dataPool;
	private int masterPort = Integer.parseInt(System.getProperty("master-port", "6379"));
	private String slaveAddress = System.getProperty("slave", "127.0.0.1");
	private int slavePort = Integer.parseInt(System.getProperty("slave-port", "6379"));

	private ValueCheck valueCheck;
	private DelayManager delayManager;

	@Before
	public void setUp() {

		maxKeys = MAX_MEMORY/msgSize;
		logger.info("config:{}", new JsonCodec(false, true).encode(this));
		valueCheckThreadNum = (producerThreadNum < valueCheckThreadNum) ? valueCheckThreadNum : producerThreadNum;
		logger.info("[ProducerThread]{} [ValueCheckThread]{}", producerThreadNum, valueCheckThreadNum);

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

		
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxTotal(producerThreadNum * 3);
		dataPool = new GenericObjectPool<>(new BytesFactory(msgSize), config);
		

		masterPool = getJedisPool(masterAddress, masterPort, producerThreadNum * 2, producerThreadNum, 2000);
		slavePool = getJedisPool(slaveAddress, slavePort, valueCheckThreadNum * 2, valueCheckThreadNum, 2000);

		if (startValueCheck) {

			logger.info("[setUp][addValueCheck]");
			valueCheck = new DefaultValueCheck(valueCheckThreadNum, slavePool);
		} else {

			logger.info("[setUp][NullValueCheck]");
			valueCheck = new NullValueCheck();
		}
		
		delayManager = new DelayManager(qpsCheckThreadPool, TIME_TOO_LONG_TO_LOG_MILLI);
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

	private class ProducerThread extends AbstractExceptionLogTask{
		
		private UnsignedLongByte key = new UnsignedLongByte();
		private UnsignedLongByte nanoTime = new UnsignedLongByte();
		private UnsignedLongByte currentMilli = new UnsignedLongByte();
		byte []preBytes = new byte[50];
		
		@Override
		public void doRun() {
			Jedis master = null;
			byte[] value = null;
			
			try {
				master = masterPool.getResource();
				
				key.from(globalCnt.getAndIncrement() % maxKeys);
				nanoTime.from(System.nanoTime());
				currentMilli.from(System.currentTimeMillis());
				
				value = dataPool.borrowObject();
				
				int preIndex = nanoTime.put(preBytes);
				preBytes[preIndex++] = '-';
				preIndex += currentMilli.put(preBytes, preIndex);
				preBytes[preIndex++] = '-';
				
				buildValue(value, preBytes, preIndex);
				
				String pre = new String(preBytes, 0, preIndex);

				records.put(key.toString(), pre);
				master.set(key.getBytes(), value);
				queryCnt.incrementAndGet();
			} catch (Exception e) {
				logger.error(String.format("[startProducerJob][run]InsertValue Exception : Key:%", key), 
						e);
				records.remove(key);
			} finally {
				if(value != null){
					try {
						dataPool.returnObject(value);
					} catch (Exception e) {
						logger.error("[run]", e);
					}
				}
				if (null != master) {
					master.close();
				}
			}
		}
	}

	private void buildValue(byte[]data, byte []preBytes, int len) {

		for (int i=0; i < len ;i++) {
			data[i] = preBytes[i];
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
				long current = System.nanoTime();
				long produceTime = Long.valueOf(value.substring(0, value.indexOf("-")));
				long delay = current - produceTime;
				catIntervalTotalDelay.addAndGet(delay);
				
				delayManager.delay(delay);
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
				Cat.logMetricForSum("xpipe.redis.queue", valueCheck.queueSize());

				logger.info("[startQpsCheckJob][run]QPS : {}", qps);
				logger.info("[startQpsCheckJob][run]MapSize : {}", records.size());
				logger.info("[startQpsCheckJob][run]QueueSize : {}", valueCheck.queueSize());
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
}
