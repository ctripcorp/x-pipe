package com.ctrip.xpipe.redis.integratedtest.stress;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

public class RedisWithKeeperStressTest {
	public static final Logger logger = LoggerFactory
			.getLogger(RedisWithKeeperStressTest.class);

	public static Properties pro;
	public static String testParamsList;
	public static String testValue;
	public static int nsInOneMs = 1000000;
	public static int checkNum = 10000;

	public static JedisPool masterPool;
	public static JedisPool slavePool;

	public AtomicLong totalDelay = new AtomicLong();
	public AtomicLong readCount = new AtomicLong();

	public long threadNum;
	public long testCount;
	public int pageSizeInOneMsSleep = 5;

	public long start = 0;
	public long end = 0;

	public long[] delayArray;
	public boolean isOver = false;
	public String channel;

	static {
		pro = new Properties();
		try {
			pro.load(new FileInputStream(
					"/opt/data/100004376/stress.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		// testParamsList=#testCount1:threadNum1:sheepTime1,testCount2:threadNum2:sheepTime2
		testParamsList = pro.getProperty("testParamsList");
		// the test value in redis
		testValue = pro.getProperty("testValue");
	}

	public static void main(String[] args) {
		String[] testParamsArr = testParamsList.split(",");
		masterPool = getPool(pro.getProperty("masterIp"), 6379, 40, 5);
		slavePool = getPool(pro.getProperty("slaveIp"), 6379, 40, 5);

		int n = 0;
		for (String testParams : testParamsArr) {
			n++;
			String[] params = testParams.split(":");
			RedisWithKeeperStressTest t = new RedisWithKeeperStressTest(
					Long.parseLong(params[0]), Long.parseLong(params[1]),
					(nsInOneMs / Integer.parseInt(params[2])));
			t.channel = UUID.randomUUID().toString();
			t.delayArray = new long[(int) (t.testCount)];

			long start = System.currentTimeMillis();
			flushAll();
			logger.error(String.format("flushAll() %d ms",
					(System.currentTimeMillis() - start)));

			t.startGetThread();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
			t.startSetThread(1);

			while (true) {
				if (t.isOver) {
					if (n == testParamsArr.length) {
						System.exit(0);
					}
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
					}
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			}
		}
	}

	public static void flushAll() {
		try {
			if (masterPool != null)
				masterPool.getResource().flushAll();
		} catch (Exception e) {
			logger.info("Read timed out for the flushAll,sleep 60000 for the redis's flushAll operation");
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}

	public RedisWithKeeperStressTest(long testCount, long threadNum,
			int pageSizeInOneMsSleep) {
		this.testCount = testCount;
		this.threadNum = threadNum;
		this.pageSizeInOneMsSleep = pageSizeInOneMsSleep;
	}

	public static JedisPool getPool(String ip, int port, int maxTotal,
			int maxIdle) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		return new JedisPool(config, ip, port);
	}

	public void startSetThread(final long millis) {
		List<Thread> theads = new ArrayList<Thread>();
		final int testCountByThread = (int) (testCount / threadNum);
		for (int i = 0; i < this.threadNum; i++) {
			theads.add(new Thread() {
				@SuppressWarnings("deprecation")
				@Override
				public void run() {
					Jedis master = masterPool.getResource();
					initStartTime(System.currentTimeMillis());
					logger.error(Thread.currentThread().getName() + ">>start");
					try {
						long start = 0;
						for (int i = 0; i < testCountByThread; i++) {
							if ((i + 1) % checkNum == 1) {
								start = System.currentTimeMillis();
							}
							String key = Long.toHexString(System.nanoTime());
							master.set(key, testValue);
							try {
								if (i % pageSizeInOneMsSleep == 0) {
									Thread.sleep(millis);
								}
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							if ((i + 1) % checkNum == 0) {
								logger.error(String.format(
										"%s>>insert %d lose:%d ms", Thread
												.currentThread().getName(),
										checkNum, System.currentTimeMillis()
												- start));
							}
						}
						initEndTime(System.currentTimeMillis());
					} catch (Exception e) {
						e.printStackTrace();
						logger.error("stress fail:", e);
						System.exit(0);
					} finally {
						// master.close();
						masterPool.returnResource(master);
					}
				}
			});
		}
		for (Thread t : theads)
			t.start();
	}

	protected synchronized void initEndTime(long end) {
		if (this.end < end)
			this.end = end;
	}

	protected synchronized void initStartTime(long start) {
		if (this.start == 0) {
			this.start = start;
		} else if (this.start > start) {
			this.start = start;
		}
	}

	public void startGetThread() {
		final RedisWithKeeperStressTest t = this;
		new Thread() {
			Jedis slave = slavePool.getResource();

			public void run() {
				slave.psubscribe(new JedisPubSub() {
					@Override
					public void onPMessage(String pattern, String channel,
							String msg) {
						long delay = System.nanoTime()
								- Long.parseLong(msg, 16);
						totalDelay.addAndGet(delay);
						long nowReadCount = readCount.incrementAndGet();
						delayArray[(int) (nowReadCount - 1)] = delay;
						if (nowReadCount % checkNum == 0) {
							logger.error("psubscribe 10000");
						}
						if (nowReadCount >= (testCount)) {
							logger.error("===========================");
							logger.error(String.format("TEST-START:%s",
									new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
											.format(new Date(t.start))));
							logger.error(String.format("TEST-END:%s",
									new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
											.format(new Date(t.end))));
							logger.error(String.format("END(ms):%d", t.end));
							logger.error(String.format("START(ms):%d", t.start));
							logger.error(String.format("totalDelay(ns):%d",
									t.totalDelay.get()));
							logger.error(String
									.format("averageDelay(ns)[totalDelay/testCount]:%d",
											t.totalDelay.get() / t.testCount));
							logger.error(String
									.format("qps(count/ms)[testCount/((end-start)/1000)]:%d",
											(t.testCount / ((t.end - t.start) / 1000))));

							Arrays.sort(t.delayArray);
							logger.error(String.format("testCount:%d",
									t.testCount));
							logger.error(String.format("minDelay(ns):%d",
									t.delayArray[0]));
							logger.error(String.format("maxDelay(ns):%d",
									t.delayArray[t.delayArray.length - 1]));
							logger.error("==========95Line===========");
							int index95 = (int) (t.delayArray.length * 0.95);
							logger.error("index95:" + index95);
							logger.error("delay95:" + t.delayArray[index95]);
							logger.error("==========95Line===========");

							logger.error("==========99Line===========");
							int index99 = (int) (t.delayArray.length * 0.99);
							logger.error("index99:" + index99);
							logger.error("delay99:" + t.delayArray[index99]);
							logger.error("==========99Line===========");

							logger.error("==========99.9Line=========");
							int index99_9 = (int) (t.delayArray.length * 0.999);
							logger.error("index99.9:" + index99_9);
							logger.error("delay99.9:" + t.delayArray[index99_9]);
							logger.error("==========99.9Line=========");
							logger.error("===========end=============");
							t.isOver = true;
						}
					}
				}, "*");// "__key*__:*"
			}
		}.start();
	}

}
