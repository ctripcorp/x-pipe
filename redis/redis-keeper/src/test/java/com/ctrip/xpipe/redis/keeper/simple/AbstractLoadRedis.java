package com.ctrip.xpipe.redis.keeper.simple;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public abstract class AbstractLoadRedis extends AbstractRedis{

	protected long total = 1 << 30;
	
	protected long maxKeyIndex = Integer.parseInt(System.getProperty("maxKeyIndex", String.valueOf(1 << 20)));

	protected int sleepMilli = Integer.parseInt(System.getProperty("sleepMilli", String.valueOf(0)));
	
	protected final AtomicLong current = new AtomicLong();

	public AbstractLoadRedis(InetSocketAddress master) {
		super(master);
	}

	public void setMaxKeyIndex(long maxKeyIndex) {
		this.maxKeyIndex = maxKeyIndex;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		
		scheduled.scheduleWithFixedDelay(new Runnable() {
			
			private long lastNum = 0;
			private long lastTimeMili = System.currentTimeMillis();
			@Override
			public void run() {
				
				long currentNum = current.get();
				long currentTime = System.currentTimeMillis();
				
				long deltaSeconds = (currentTime - lastTimeMili)/1000; 
				if(deltaSeconds > 0){
					logger.info("[SEND RATE]{}", (currentNum - lastNum)/deltaSeconds);
				}
				
				lastNum = currentNum;
				lastTimeMili = currentTime;
			}
		}, 5, 5, TimeUnit.SECONDS);
	}
	
	
	protected long getKeyIndex(long index) {
		return index%maxKeyIndex;
	}

	public long increase(){
		
		long next = current.incrementAndGet();
		if(next > total){
			current.decrementAndGet();
			return -1;
		}
		return next; 
	}
}
