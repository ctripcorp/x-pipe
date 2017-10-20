package com.ctrip.xpipe.redis.console.monitor.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.console.exception.LinkRouteBrokenException;
import com.ctrip.xpipe.redis.console.monitor.AbstractStatMonitor;
import com.ctrip.xpipe.redis.console.monitor.StatMonitor;
import com.ctrip.xpipe.redis.console.monitor.statmodel.StandaloneStatModel;
import com.ctrip.xpipe.redis.console.monitor.statmodel.StandaloneStatModel.StandaloneClusterStat;
import com.ctrip.xpipe.redis.console.monitor.statmodel.StandaloneStatModel.StandaloneRedisStat;
import com.ctrip.xpipe.redis.console.monitor.statmodel.StandaloneStatModel.StandaloneShardStat;
import com.ctrip.xpipe.utils.FileUtils;
import com.dianping.cat.Cat;
import com.google.common.io.CharStreams;
import org.xml.sax.SAXException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class StandaloneStatMonitor extends AbstractStatMonitor implements StatMonitor, Runnable {
	public static final String TEST_KEY = "xpipe-test";

	private StandaloneStatModel standaloneStat;
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
	private ExecutorService fixedThreadPool = Executors.newFixedThreadPool(20);
	private Long ALERT_INTERVAL_MILLS = Long.parseLong(System.getProperty("alert-interval", "5000"));

	private ConcurrentHashMap<StandaloneRedisStat, Boolean> redisStatCheckResult = new ConcurrentHashMap<>();

	public StandaloneStatMonitor(String configFile) throws UnsupportedEncodingException, IOException {
		this(configFile, 5000L, TimeUnit.MILLISECONDS);
	}

	public StandaloneStatMonitor(String configFile, long period, TimeUnit timeUnit)
			throws IOException {
		standaloneStat = loadStatModel(configFile);
		scheduled.scheduleAtFixedRate(this, 0, period, timeUnit);
	}

	private StandaloneStatModel loadStatModel(String configFile) throws IOException {
		InputStream ins = FileUtils.getFileInputStream(configFile);
		return Codec.DEFAULT.decode(CharStreams.toString(new InputStreamReader(ins, "UTF-8")),
				StandaloneStatModel.class);
	}

	@Override
	public void run() {
		Jedis master = null;
		List<Jedis> jedisSlaves = new LinkedList<>();
		
		for(StandaloneClusterStat cluster : standaloneStat.getClusterStats().values()) {
			for(StandaloneShardStat shard : cluster.getShardStats().values()) {
				try {
					master = new Jedis(shard.getRedisMaster().getIp(), shard.getRedisMaster().getPort());
					for(StandaloneRedisStat redis : shard.getRedisSlaves()) {
						if(null == redisStatCheckResult.get(redis)) {
							redisStatCheckResult.put(redis, Boolean.FALSE);
						}
						
						Jedis slave = new Jedis(redis.getIp(), redis.getPort());
						fixedThreadPool.submit(new SlaveListenJob(shard.getRedisMaster(), redis, slave));
						jedisSlaves.add(slave);
					}
					TimeUnit.MILLISECONDS.sleep(500);
					
					master.publish(generateURL(shard.getRedisMaster().getIp(), shard.getRedisMaster().getPort()),
							TEST_KEY);
					logger.debug("[Master][publish]{}-{},{}:{}",cluster.getClusterId(), shard.getShardId(), shard.getRedisMaster().getIp(), shard.getRedisMaster().getPort());
					
					TimeUnit.MILLISECONDS.sleep(ALERT_INTERVAL_MILLS);
					for(StandaloneRedisStat redis : shard.getRedisSlaves()) {
						if(redisStatCheckResult.get(redis).equals(Boolean.FALSE)) {
							logger.error("[Fail]Fail on cluster:{}, shard:{}, redis:{}:{}",cluster.getClusterId(), shard.getShardId(),
									redis.getIp(), redis.getPort());
							Cat.logError(new LinkRouteBrokenException(String.format("LinkRouteBroken:%s-%s-%s:%s", 
									cluster.getClusterId(), shard.getShardId(), redis.getIp(), redis.getPort()) ));
							
						} else {
							logger.info("[Success]Success on cluster:{}, shard:{}, redis:{}:{}", cluster.getClusterId(), shard.getShardId(),
									redis.getIp(), redis.getPort());
							redisStatCheckResult.put(redis, Boolean.FALSE);
						}
					}
				} catch (Exception ex) {
					logger.error("[Unexpected error]cluster:{}, shard:{}",cluster.getClusterId(), shard.getShardId(), ex);
					Cat.logError(new LinkRouteBrokenException(String.format("[Unknown redis]LinkRouteBroken:%s-%s", 
							cluster.getClusterId(), shard.getShardId())));
				} finally {
					if(null != master) master.close();
					for(Jedis slave : jedisSlaves) {
						if(null != slave) slave.close();
					}
					jedisSlaves.clear();
				}
			}
		}
	}

	private class SlaveListenJob implements Runnable {
		private StandaloneRedisStat masterRedis;
		private StandaloneRedisStat slaveRedis;
		private Jedis slave;

		public SlaveListenJob(StandaloneRedisStat masterRedis,StandaloneRedisStat slaveRedis, Jedis slave) {
			logger.debug("[SlaveListenerJob]{}:{}", slaveRedis.getIp(), slaveRedis.getPort());
			this.masterRedis = masterRedis;
			this.slaveRedis = slaveRedis;
			this.slave = slave;
		}

		@SuppressWarnings("static-access")
		@Override
		public void run() {
			Thread.currentThread().setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
				@Override
				public void uncaughtException(Thread arg0, Throwable arg1) {
					logger.error("[error]{}:{}",slaveRedis.getIp(), slaveRedis.getPort(), arg1);
					Cat.logError(arg1);
					redisStatCheckResult.put(slaveRedis, Boolean.FALSE);
					if (null != slave) {
						slave.close();
					}
				}
			});

			logger.debug("[Psubscribe]{}:{}", slaveRedis.getIp(), slaveRedis.getPort());
			slave.psubscribe(new JedisPubSub() {
				@Override
				public void onPMessage(String pattern, String channel, String msg) {
					logger.debug("[OnPMessage]{}:{}", slaveRedis.getIp(), slaveRedis.getPort());
					redisStatCheckResult.put(slaveRedis, Boolean.TRUE);
				}
			}, generateURL(masterRedis.getIp(), masterRedis.getPort()));
		}
	}
	
	private String generateURL(String ip, int port) {
		return ip + ":" + String.valueOf(port);
	}

	public static void main(String[] args) throws SAXException, IOException, InterruptedException {
		new StandaloneStatMonitor("standalone-stat-monitor-test.json");
		TimeUnit.DAYS.sleep(31);
	}
}
