package com.ctrip.xpipe.redis.keeper.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.redis.keeper.entity.Keeper;
import com.ctrip.xpipe.redis.keeper.entity.Redis;
import com.ctrip.xpipe.utils.OsUtils;


/**
 * @author wenchao.meng
 *
 * Jun 1, 2016
 */
@Component
public class DefaultMetaServiceManager extends AbstractObservable implements MetaServiceManager{
	
	public static int META_GET_INTERVAL = 5;
	
	@Autowired
	private MetaService  metaService;
	
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount() * 10);
	
	private Map<String, Pair<ScheduledFuture<?>, ScheduledFuture<?>>>  workers = new ConcurrentHashMap<>();
	

	@Override
	public synchronized void removeShard(String clusterId, String shardId) {
		
		Pair<ScheduledFuture<?>, ScheduledFuture<?>> worker  = workers.remove(getKey(clusterId, shardId));
		if(worker == null){
			logger.info("[removeShard][notexist]{}, {}", clusterId, shardId);
			return;
		}
		
		worker.getKey().cancel(false);
		worker.getValue().cancel(false);
	}

	@Override
	public synchronized void addShard(String clusterId, String shardId) {
		
		Pair<ScheduledFuture<?>, ScheduledFuture<?>> worker = getWorker(clusterId, shardId);
		if(worker != null){
			logger.info("[addShard][already exist]{}, {}", clusterId, shardId);
			return;
		}
		
		ScheduledFuture<?> futureKeeper = scheduled.scheduleWithFixedDelay(new KeeperWorker(clusterId, shardId), 0, META_GET_INTERVAL, TimeUnit.SECONDS);
		ScheduledFuture<?> futureRedis = scheduled.scheduleWithFixedDelay(new RedisWorker(clusterId, shardId), 0, META_GET_INTERVAL, TimeUnit.SECONDS);;
		workers.put(getKey(clusterId, shardId), new Pair<ScheduledFuture<?>, ScheduledFuture<?>>(futureKeeper, futureRedis));
	}

	private Pair<ScheduledFuture<?>, ScheduledFuture<?>> getWorker(String clusterId, String shardId) {
		
		String key = getKey(clusterId, shardId);
		return workers.get(key);
		
	}

	private String getKey(String clusterId, String shardId) {
		return clusterId + "-" + shardId;
	}
	
	abstract class AbstractWorker implements Runnable{
		
		protected String clusterId, shardId;
		
		public AbstractWorker(String clusterId, String shardId){
			this.clusterId = clusterId;
			this.shardId = shardId;
			
		}
		
		public void run() {
			try{
				doRun();
			}catch(Exception e){
				logger.error("[run]" + clusterId + "," + shardId, e);
			}
			
		}
		
		public abstract void doRun();
	}

	class KeeperWorker extends AbstractWorker{

		public KeeperWorker(String clusterId, String shardId) {
			super(clusterId, shardId);
		}

		@Override
		public void doRun() {
			
			Keeper keeper = metaService.getActiveKeeper(clusterId, shardId);
			if(keeper != null){
				notifyObservers(new MetaUpdateInfo(clusterId, shardId, keeper));
			}
		}
	}
	
	class RedisWorker extends AbstractWorker{

		public RedisWorker(String clusterId, String shardId) {
			super(clusterId, shardId);
		}

		@Override
		public void doRun() {
			
			Redis redis = metaService.getRedisMaster(clusterId, shardId);
			if(redis != null){
				notifyObservers(new MetaUpdateInfo(clusterId, shardId, redis));
			}
		}
		
	}
	
	public static class MetaUpdateInfo{
		
		private String clusterId, shardId;
		private Object info;
		
		public MetaUpdateInfo(String clusterId, String shardId, Object info){
			this.clusterId = clusterId;
			this.shardId = shardId;
			this.info = info;
		}
		
		
		public String getClusterId() {
			return clusterId;
		}

		public String getShardId() {
			return shardId;
		}

		public Object getInfo() {
			return info;
		}
	}

}
