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
	
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount());
	
	private Map<Pair<String, String>, MetaInfo>  metaInfos = new ConcurrentHashMap<>();
	
	@Override
	public synchronized void removeShard(String clusterId, String shardId) {
		
		MetaInfo metaInfo  = metaInfos.remove(getKey(clusterId, shardId));
		if(metaInfo == null){
			logger.warn("[removeShard][notexist]{}, {}", clusterId, shardId);
			return;
		}
		metaInfo.getFuture().cancel(true);
	}

	@Override
	public synchronized void addShard(String clusterId, String shardId) {
		
		MetaInfo metaInfo = getMetaInfo(clusterId, shardId);
		if(metaInfo != null){
			logger.info("[addShard][already exist]{}, {}", clusterId, shardId);
			return;
		}
		
		ScheduledFuture<?> futureShard = scheduled.scheduleWithFixedDelay(new ShardWorker(clusterId, shardId), 0, META_GET_INTERVAL, TimeUnit.SECONDS);
		metaInfos.put(getKey(clusterId, shardId), new MetaInfo(futureShard));
	}
	
	@Override
	public ShardStatus getShardStatus(String clusterId, String shardId) {
		
		MetaInfo metaInfo = getMetaInfo(clusterId, shardId);
		if( metaInfo == null){
			return null;
		}
		return metaInfo.getShardStatus();
	}


	private MetaInfo getMetaInfo(String clusterId, String shardId) {
		
		Pair<String, String> key = getKey(clusterId, shardId);
		return metaInfos.get(key);
		
	}

	private Pair<String, String> getKey(String clusterId, String shardId) {
		return new Pair<>(clusterId, shardId);
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

	class ShardWorker extends AbstractWorker{

		public ShardWorker(String clusterId, String shardId) {
			super(clusterId, shardId);
		}

		@Override
		public void doRun() {
			
			ShardStatus shardStatus = metaService.getShardStatus(clusterId, shardId);
			if(shardStatus == null){
				logger.warn("[doRun][shardStatus null]{}, {}", clusterId, shardId);
				return;
			}
			
			MetaInfo metaInfo = getMetaInfo(clusterId, shardId);
			
			if(metaInfo == null){
				return;
			}

			if(metaInfo.updateIfNotEqual(shardStatus)){
				logger.info("[doRun][shardStatusChanged]{}", shardStatus);
				notifyObservers(new MetaUpdateInfo(clusterId, shardId, shardStatus));
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
	
	
	public static class MetaInfo{
		
		private ShardStatus shardStatus;
		private final ScheduledFuture<?> future;
		
		public MetaInfo(ScheduledFuture<?> future){
			this.future = future;
		}
		
		public boolean updateIfNotEqual(ShardStatus shardStatus) {
			if(shardStatus == null){
				return false;
			}
			
			if(this.shardStatus == null || !this.shardStatus.equals(shardStatus)){
				this.shardStatus = shardStatus;
				return true;
			}
			return false;
		}
		
		public ScheduledFuture<?> getFuture() {
			return future;
		}
		
		public ShardStatus getShardStatus() {
			return shardStatus;
		}
	}
}
