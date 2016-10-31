package com.ctrip.xpipe.redis.integratedtest.function.stress;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
 * @author liu
 * 
 *         Oct 9, 2016
 */
public abstract class AbstractStress implements Thread.UncaughtExceptionHandler {
	protected static Logger logger = LoggerFactory
			.getLogger(AbstractStress.class);

	public static final String BASE_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
	public static final int BASE_CHARS_LENGTH = 36;

	public static int nsInOneMs = 1000000;

	protected JedisPool masterPool;
	protected String masterIp;
	protected int masterPort;
	protected JedisPool slavePool;
	protected String slaveIp;
	protected int slavePort;

	protected AtomicLong totalDelay;
	protected AtomicLong readCount;

	protected long threadNum;
	protected long testCount;
	protected int valueLength = 20;
	protected String value;
	protected int numberPerMillisecond;
	protected int pageSizeInOneMsSleep = 5;

	protected long startTimeStamp = 0;
	protected long endTimeStamp = 0;

	protected long[] delayArray;
	protected String channel;

	CountDownLatch latch = new CountDownLatch(1);

	public AbstractStress(long testCount, long threadNum,
			int numberPerMillisecond, String masterIp, int masterPort,
			String slaveIp, int slavePort, int valueLength) {
		this.testCount = testCount;
		this.threadNum = threadNum;
		this.numberPerMillisecond = numberPerMillisecond;
		this.masterPool = getPool(masterIp, masterPort, getMasterMaxTotal(),
				getMasterMaxIdle());
		this.slavePool = getPool(slaveIp, slavePort, getSlaveMaxTotal(),
				getSlaveMaxIdle());
		this.channel = getChannel();
		this.value = getRandomString(valueLength);
		totalDelay = new AtomicLong(0);
		readCount = new AtomicLong(0);
	}

	public void startTest() {
		this.delayArray = new long[(int) (this.testCount)];
		long start = System.currentTimeMillis();

		flushAll();
		logger.info(String.format("flushAll() %d ms",
				(System.currentTimeMillis() - start)));

		this.startGetThread();
		// wait JedisPubSub client startup
		sleep(1000);
		this.startSetThread(1);

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("[startTest]InterruptedException", e);
		}
		sleep(2 * 1000);
		close();
	}

	private void close() {
		try {
			if (masterPool != null) {
				masterPool.close();
			}
			if (slavePool != null) {
				slavePool.close();
			}
		} catch (Exception e) {
			logger.error("[close]Exception",e);
		}
	}

	public void startSetThread(final long millis) {
		List<Thread> theads = new ArrayList<Thread>();
		final int testCountByThread = (int) (testCount / threadNum);
		for (int i = 0; i < this.threadNum; i++) {
			theads.add(new Thread() {
				private int runCount = 0;

				@SuppressWarnings("deprecation")
				@Override
				public void run() {
					Jedis master = masterPool.getResource();
					initStartTime(System.currentTimeMillis());
					logger.info(Thread.currentThread().getName() + ">>start");
					try {
						long start = 0;
						for (int i = runCount; i < testCountByThread; i++) {
							if ((i + 1) % 10000 == 1) {
								start = System.currentTimeMillis();
							}
							String key = Long.toHexString(System.nanoTime());
							operation(master, key, value);
							runCount = i;
							if (i % pageSizeInOneMsSleep == 0) {
								TimeUnit.MILLISECONDS.sleep(1);
							}

							if ((i + 1) % 10000 == 0) {
								logger.debug(String.format(
										"%s>>insert 10000 lose:%d ms", Thread
												.currentThread().getName(),
										System.currentTimeMillis() - start));
							}
						}
						initEndTime(System.currentTimeMillis());
					} catch (Exception e) {
						logger.error("[startSetThread][run][exception]", e);
						masterPool.returnResource(master);
						run();
					} finally {
						masterPool.returnResource(master);
					}
				}
			});
		}
		for (Thread t : theads) {
			t.setName("SET-THREAD");
			t.setUncaughtExceptionHandler(this);
			t.start();
		}
	}

	public void startGetThread() {
		final AbstractStress t = this;
		Thread getThread = new Thread() {
			Jedis slave = slavePool.getResource();

			public void run() {
				slave.psubscribe(new JedisPubSub() {
					@SuppressWarnings("deprecation")
					@Override
					public void onPMessage(String pattern, String channel,
							String msg) {
							long delay = System.nanoTime()
									- Long.parseLong(msg, 16);
							totalDelay.addAndGet(delay);
							long nowReadCount = readCount.incrementAndGet();
							delayArray[(int) (nowReadCount - 1)] = delay;
							if (nowReadCount % 10000 == 0) {
								logger.debug("psubscribe 10000 success");
							}
							if (nowReadCount >= (testCount)) {
								resultStatistics();
							}
					}

					private void resultStatistics() {
						logger.info("===========================");
						logger.info(String.format("TEST-START:%s",
								new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
										.format(new Date(t.startTimeStamp))));
						logger.info(String.format("TEST-END:%s",
								new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
										.format(new Date(t.endTimeStamp))));
						logger.info(String.format("END(ms):%d", t.endTimeStamp));
						logger.info(String.format("START(ms):%d",
								t.startTimeStamp));
						logger.info(String.format("totalDelay(ns):%d",
								t.totalDelay.get()));
						logger.info(String.format(
								"averageDelay(ns)[totalDelay/testCount]:%d",
								t.totalDelay.get() / t.testCount));
						logger.info(String
								.format("qps(count/ms)[testCount/((end-start)/1000)]:%d",
										(t.testCount / ((t.endTimeStamp
												- t.startTimeStamp) / 1000))));

						Arrays.sort(t.delayArray);
						logger.info(String.format("testCount:%d", t.testCount));
						logger.info(String.format("minDelay(ns):%d",
								t.delayArray[0]));
						logger.info(String.format("maxDelay(ns):%d",
								t.delayArray[t.delayArray.length - 1]));
						logger.info("==========95Line===========");
						int index95 = (int) (t.delayArray.length * 0.95);
						logger.info("index95:" + index95);
						logger.info("delay95:" + t.delayArray[index95]);
						logger.info("==========95Line===========");

						logger.info("==========99Line===========");
						int index99 = (int) (t.delayArray.length * 0.99);
						logger.info("index99:" + index99);
						logger.info("delay99:" + t.delayArray[index99]);
						logger.info("==========99Line===========");

						logger.info("==========99.9Line=========");
						int index99_9 = (int) (t.delayArray.length * 0.999);
						logger.info("index99.9:" + index99_9);
						logger.info("delay99.9:" + t.delayArray[index99_9]);
						logger.info("==========99.9Line=========");
						logger.info("===========end=============");

						latch.countDown();
						//slavePool.returnResource(slave);
					}
				}, channel);
			}
		};
		getThread.setName("GET-THREAD");
		getThread.setUncaughtExceptionHandler(this);
		getThread.start();
	}

	protected synchronized void initEndTime(long endTimeStamp) {
		if (this.endTimeStamp < endTimeStamp)
			this.endTimeStamp = endTimeStamp;
	}

	protected synchronized void initStartTime(long startTimeStamp) {
		if (this.startTimeStamp == 0) {
			this.startTimeStamp = startTimeStamp;
		} else if (this.startTimeStamp > startTimeStamp) {
			this.startTimeStamp = startTimeStamp;
		}
	}

	private JedisPool getPool(String ip, int port, int maxTotal, int maxIdle) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		return new JedisPool(config, ip, port);
	}

	@SuppressWarnings("deprecation")
	private void flushAll() {
		Jedis master = masterPool.getResource();
		try {
			logger.info("flushAll");
			master.flushAll();
		} catch (Exception e) {
			logger.warn("[flushAll]acquiring the synchronization results failure,now start waitting 60ms");
			sleep(60 * 1000);
		} finally {
			masterPool.returnResourceObject(master);
		}
	}

	protected static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			logger.warn("[sleep][InterruptedException]", e);
		}
	}

	public static String getRandomString(int length) {
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < length; i++) {
			int number = random.nextInt(BASE_CHARS_LENGTH);
			sb.append(BASE_CHARS.charAt(number));
		}
		return sb.toString();
	}

	abstract protected int getMasterMaxTotal();

	abstract protected int getMasterMaxIdle();

	abstract protected int getSlaveMaxTotal();

	abstract protected int getSlaveMaxIdle();

	abstract protected String getChannel();

	abstract protected void operation(Jedis master, String key, String value);

	public void uncaughtException(Thread t, Throwable e) {
		logger.error("ThreadException"+t.getName()+"-"+t.getId(), e);
	}
}
