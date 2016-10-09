package com.ctrip.xpipe.redis.integratedtest.function.stress;

import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.utils.StringUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;


/**
 * @author liu
 *
 * Oct 9, 2016
 */
public abstract class AbstractStress {

	public static final Logger logger = LoggerFactory
			.getLogger(AbstractStress.class);
	public static final String BASE_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
	public static final int BASE_CHARS_LENGTH = 36;
	public static Properties pro;
	public static String testParamsList;
	public static int valueLength = 20;
	public static int nsInOneMs = 1000000;
	public static int checkNum = 10000;

	public static JedisPool masterPool;
	public static JedisPool slavePool;

	public AtomicLong totalDelay;
	public AtomicLong readCount;

	public long threadNum;
	public long testCount;
	public int pageSizeInOneMsSleep = 5;

	public long start = 0;
	public long end = 0;

	public long[] delayArray;
	public String channel;

	public final static Object WAIT_OR_NOTIFY_LOCK = new Object();
	static {
		pro = new Properties();
		try {
			pro.load(new FileInputStream(
					"/opt/data/100004376/stress.properties"));
			// testParamsList=testCount1:threadNum1:sheepTime1,testCount2:threadNum2:sheepTime2
			testParamsList = pro.getProperty("testParamsList");
			// the test value length
			valueLength = Integer.parseInt(pro.getProperty("valueLength"));
		} catch (Exception e) {
			logger.error("[static] init stress.properties exception", e);
		}

	}

	public void startTest() {
		String[] testParamsArr = StringUtil.isEmpty(testParamsList) ? new String[] {}
				: testParamsList.split(",");
		if (testParamsArr.length <= 0) {
			logger.warn("testParamsList is null");
			return;
		}
		masterPool = getPool(pro.getProperty("masterIp"), 6379, 40, 5);
		slavePool = getPool(pro.getProperty("slaveIp"), 6379, 40, 5);

		int n = 0;
		for (String testParams : testParamsArr) {
			n++;
			String[] params = testParams.split(":");

			init(Long.parseLong(params[0]), Long.parseLong(params[1]),
					(nsInOneMs / Integer.parseInt(params[2])));
			this.channel = UUID.randomUUID().toString();
			this.delayArray = new long[(int) (this.testCount)];

			long start = System.currentTimeMillis();

			flushAll();
			logger.info(String.format("flushAll() %d ms",
					(System.currentTimeMillis() - start)));

			this.startGetThread();
			// wait JedisPubSub client startup
			sleep(1000);
			this.startSetThread(1);

			while (true) {
				synchronized (AbstractStress.WAIT_OR_NOTIFY_LOCK) {
					try {
						AbstractStress.WAIT_OR_NOTIFY_LOCK.wait();
					} catch (InterruptedException e) {
						logger.error("InterruptedException",e);
					}
				}
				if (n == testParamsArr.length) {
					System.exit(0);
				}
				sleep(5000);
				break;
			}
		}
	}

	@SuppressWarnings("deprecation")
	public static void flushAll() {
		Jedis master = masterPool.getResource();
		try {
			master.flushAll();
		} catch (Exception e) {
			logger.warn("[flushAll]acquiring the synchronization results failure,now start waitting 60ms");
			sleep(10000);
		} finally {
			masterPool.returnResourceObject(master);
		}
	}

	public AbstractStress(long testCount, long threadNum,
			int pageSizeInOneMsSleep) {
		this.testCount = testCount;
		this.threadNum = threadNum;
		this.pageSizeInOneMsSleep = pageSizeInOneMsSleep;
	}

	public AbstractStress() {
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
					logger.info(Thread.currentThread().getName() + ">>start");
					try {
						long start = 0;
						for (int i = 0; i < testCountByThread; i++) {
							if ((i + 1) % checkNum == 1) {
								start = System.currentTimeMillis();
							}
							String key = Long.toHexString(System.nanoTime());
							String value = getRandomString(100);
							// master.set(key, getRandomString(100));
							operation(master, key, value);
							if (i % pageSizeInOneMsSleep == 0) {
								sleep(millis);
							}

							if ((i + 1) % checkNum == 0) {
								logger.debug(String.format(
										"%s>>insert %d lose:%d ms", Thread
												.currentThread().getName(),
										checkNum, System.currentTimeMillis()
												- start));
							}
						}
						initEndTime(System.currentTimeMillis());
					} catch (Exception e) {
						logger.error("[startSetThread][run]", e);
						System.exit(0);
					} finally {
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
		final AbstractStress t = this;
		new Thread() {
			Jedis slave = slavePool.getResource();

			public void run() {
				/* try { */
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
						if (nowReadCount % checkNum == 0) {
							logger.debug("psubscribe 10000");
						}
						if (nowReadCount >= (testCount)) {
							logger.info("===========================");
							logger.info(String.format("TEST-START:%s",
									new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
											.format(new Date(t.start))));
							logger.info(String.format("TEST-END:%s",
									new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
											.format(new Date(t.end))));
							logger.info(String.format("END(ms):%d", t.end));
							logger.info(String.format("START(ms):%d", t.start));
							logger.info(String.format("totalDelay(ns):%d",
									t.totalDelay.get()));
							logger.info(String
									.format("averageDelay(ns)[totalDelay/testCount]:%d",
											t.totalDelay.get() / t.testCount));
							logger.info(String
									.format("qps(count/ms)[testCount/((end-start)/1000)]:%d",
											(t.testCount / ((t.end - t.start) / 1000))));

							Arrays.sort(t.delayArray);
							logger.info(String.format("testCount:%d",
									t.testCount));
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
							synchronized (AbstractStress.WAIT_OR_NOTIFY_LOCK) {
								AbstractStress.WAIT_OR_NOTIFY_LOCK.notify();
							}
							slavePool.returnResource(slave);
						}
					}
				}, getOnPMessageChannel());// "__key*__:*"
			}
		}.start();
	}

	protected static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			logger.warn("[sleep]InterruptedException", e);
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

	abstract protected String getOnPMessageChannel();

	abstract protected void operation(Jedis master, String key, String value);

	protected void init(long testCount, long threadNum, int pageSizeInOneMsSleep) {
		this.testCount = testCount;
		this.threadNum = threadNum;
		this.pageSizeInOneMsSleep = pageSizeInOneMsSleep;
		this.readCount = new AtomicLong();
		this.totalDelay = new AtomicLong();
	};
}
