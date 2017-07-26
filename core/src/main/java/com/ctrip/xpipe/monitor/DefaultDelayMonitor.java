package com.ctrip.xpipe.monitor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.monitor.DelayMonitor;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;

/**
 * @author wenchao.meng
 *
 * May 21, 2016 10:14:33 PM
 */
public class DefaultDelayMonitor extends AbstractStartStoppable implements DelayMonitor, Runnable{

	protected Logger logger = LoggerFactory.getLogger(getClass());

	private ScheduledExecutorService scheduled;
	private AtomicLong totalDelay = new AtomicLong();
	private AtomicLong totalNum = new AtomicLong();
	
	private long previousDelay = 0, previousNum = 0;
	private String delayType, delayInfo;

	private long infoDelta = 1000;

	private boolean consolePrint = false;

	private long max, maxTime;

	public DefaultDelayMonitor(String delayType) {
		this(delayType, 1000);
	}

	public DefaultDelayMonitor(String delayType, long infoDelta) {
		this.delayType = delayType;
		this.infoDelta = infoDelta;
	}

	@Override
	public void setConsolePrint(boolean consolePrint) {
		this.consolePrint = consolePrint;
	}

	@Override
	protected void doStart() throws Exception {
		
		scheduled = Executors.newScheduledThreadPool(4);
		ScheduledFuture<?> future = scheduled.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					future.get();
				} catch (Exception e) {
					logger.error("[doStart]", e);
				}
			}
		}).start();
	}
	
	
	@Override
	protected void doStop() throws Exception {
		scheduled.shutdown();
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
		if(delta >= 0){
			totalDelay.addAndGet(delta);
			totalNum.incrementAndGet();
		}

		if(delta > max){
			max = delta;
			maxTime = System.currentTimeMillis();
		}
	}
		
	@Override
	public void run() {
		
		try{
			long currentDelay = totalDelay.get();
			long currentNum = totalNum.get();
			
			long deltaNum = currentNum - previousNum;
			
			if(deltaNum  > 0 ){
				double avgDelay =  (double)(currentDelay - previousDelay)/deltaNum;

				logger.info(String.format("%d - %d = %d, %d - %d = %d", currentDelay,previousDelay, currentDelay - previousDelay, currentNum, previousNum, currentNum - previousNum));
				String info = String.format("[delay]%s, %s, %s", getDelayType(), delayInfo == null ? "" :delayInfo, String.format("%.2f", avgDelay));
				if(consolePrint){
					System.out.println(info);
				}
				logger.info(info);
			}

			String maxInfo = String.format("[max]%d, %s", max, DateTimeUtils.timeAsString(maxTime));
			logger.info(maxInfo);
			if(consolePrint){
				System.out.println(maxInfo);
			}

			previousDelay = currentDelay;
			previousNum = currentNum;
			max = 0;
		}catch(Throwable th){
			logger.error("[run]", th);
		}
	}


	@Override
	public String getDelayType() {
		return delayType;
	}

	@Override
	public void setDelayInfo(String delayInfo) {
		this.delayInfo = delayInfo;
	}
}
