package com.ctrip.xpipe.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.monitor.DelayMonitor;

/**
 * @author wenchao.meng
 *
 * May 21, 2016 10:14:33 PM
 */
public class DefaultDelayMonitor implements DelayMonitor, Runnable{

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private ScheduledExecutorService scheduled;
	private AtomicLong totalDelay = new AtomicLong();
	private AtomicLong totalNum = new AtomicLong();
	
	private long previousDelay = 0, previousNum = 0;
	private String delayType;

	private long infoDelta = 1000;

	public DefaultDelayMonitor(String delayType) {
		this(delayType, 1000);
	}

	public DefaultDelayMonitor(String delayType, long infoDelta) {
		this.delayType = delayType;
		this.infoDelta = infoDelta;
		scheduled = Executors.newScheduledThreadPool(4);
		scheduled.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
	}
	

	@Override
	public void addData(long lastTime) {
		
		if(lastTime < 0 ){
			return;
		}
		
		long current = System.currentTimeMillis(); 
		long delta =  current - lastTime;
		if(delta > infoDelta){
			logger.info("{} - {} = {}", current, lastTime, delta);
		}
		if(delta > 0){
			totalDelay.addAndGet(delta);
			totalNum.incrementAndGet();
		}
	}
		
	@Override
	public void run() {
		
		long currentDelay = totalDelay.get();
		long currentNum = totalNum.get();
		
		long deltaNum = currentNum - previousNum;
		if(deltaNum  > 0 ){
			System.out.println(String.format("%d - %d = %d, %d - %d = %d", currentDelay,previousDelay, currentDelay - previousDelay, currentNum, previousNum, currentNum - previousNum));
			System.out.println((currentDelay - previousDelay)/deltaNum);
			logger.info("[delay]{} {}", getDelayType(), (currentDelay - previousDelay)/deltaNum);
		}
		
		previousDelay = currentDelay;
		previousNum = currentNum;
	}


	@Override
	public String getDelayType() {
		return delayType;
	}
}
