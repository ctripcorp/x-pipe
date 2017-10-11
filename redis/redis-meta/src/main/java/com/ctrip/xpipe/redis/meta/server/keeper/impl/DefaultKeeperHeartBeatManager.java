package com.ctrip.xpipe.redis.meta.server.keeper.impl;


import com.ctrip.xpipe.observer.AbstractObservable;
import com.ctrip.xpipe.observer.NodeAdded;
import com.ctrip.xpipe.observer.NodeDeleted;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperHeartBeatManager;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public class DefaultKeeperHeartBeatManager extends AbstractObservable implements KeeperHeartBeatManager{
	
	private Long lastHeartBeatTime = System.currentTimeMillis();
	
	private ScheduledExecutorService scheduledExecutor;
	
	private volatile ScheduledFuture<?> timeOutFuture;
	
	private volatile AtomicBoolean isAlive = new AtomicBoolean(true);
	
	private KeeperKey keeperKey;
	
	public DefaultKeeperHeartBeatManager(KeeperKey keeperKey, ScheduledExecutorService scheduledExecutor) {
		
		this.keeperKey = keeperKey;
		this.scheduledExecutor = scheduledExecutor;
		scheduleTimeout();
	}

	private void scheduleTimeout() {
		
		timeOutFuture = scheduledExecutor.schedule(new TimeoutAction(), HEART_BEAT_INTERVAL_MILLI * 3, TimeUnit.MILLISECONDS);
	}

	@Override
	public void ping(KeeperInstanceMeta keeperInstanceMeta) {
		
		lastHeartBeatTime = System.currentTimeMillis();
		if(isAlive.compareAndSet(false, true)){
			keeperAlive();
		}
		cancelFuture();
		scheduleTimeout();
	}


	private void cancelFuture() {
		
		if(timeOutFuture != null){
			timeOutFuture.cancel(true);
			timeOutFuture = null;
		}
	}

	@Override
	public boolean isKeeperAlive() {
		return isAlive.get();
	}

	class TimeoutAction implements Runnable{

		@Override
		public void run() {
			try{
				keeperDead();
			}catch(Throwable th){
				logger.error("[run]", th);
			}
		}
	}

	public void keeperDead() {

		isAlive.set(false);
		logger.info("[keeperDead]{}", keeperKey);
		notifyObservers(new NodeDeleted<KeeperKey>(keeperKey));
		scheduleTimeout();
	}

	private void keeperAlive() {
		
		logger.info("[keeperAlive]{}", keeperKey);
		notifyObservers(new NodeAdded<KeeperKey>(keeperKey));
	}
	
	public Long getLastHeartBeatTime() {
		return lastHeartBeatTime;
	}


	@Override
	public void close(){
		cancelFuture();
	}
}
