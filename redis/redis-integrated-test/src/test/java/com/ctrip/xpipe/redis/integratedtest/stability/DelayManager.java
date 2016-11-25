package com.ctrip.xpipe.redis.integratedtest.stability;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;

/**
 * @author wenchao.meng
 *
 * Nov 25, 2016
 */
public class DelayManager{
	
	private static Logger logger = LoggerFactory.getLogger(DelayManager.class);
	
	private int delayPrintInterval = Integer.parseInt(System.getProperty("delay-print-interval", "5000"));
	
	private ScheduledExecutorService scheduled;
	
	private int tooLongBaseMilli;
	
	private AtomicLong tooLongCount = new AtomicLong();
	private long maxDelayNanos = 0;
	
	private AtomicLong totalCount = new AtomicLong();
	private AtomicLong totalDelay = new AtomicLong();
	
	public DelayManager(ScheduledExecutorService scheduled, int tooLongBaseMilli){
		this.scheduled = scheduled;
		this.tooLongBaseMilli = tooLongBaseMilli;
		this.scheduled.scheduleAtFixedRate(new DelayPrinter(), delayPrintInterval, delayPrintInterval, TimeUnit.MILLISECONDS);
	}
	
	public void delay(long delayNanos){
		
		totalCount.incrementAndGet();
		totalDelay.addAndGet(delayNanos);
		
		if(delayNanos >= tooLongBaseMilli * 1000000){
			tooLongCount.incrementAndGet();
		}
		
		if(delayNanos > maxDelayNanos){
			maxDelayNanos = delayNanos;
		}
	}

	public class DelayPrinter extends AbstractExceptionLogTask implements Runnable{
		
		private long previousTotalCount = 0;

		private long previousTotalDelay = 0;
		
		private long previousTooLongCount = 0;

		@Override
		protected void doRun() throws Exception {
			
			long currentTotalCount = totalCount.get();
			long currentTotalDelay = totalDelay.get();
			long currentTooLongCount = tooLongCount.get();

			long countDelta = currentTotalCount - previousTotalCount;

			long average = countDelta == 0 ? 0 : (currentTotalDelay - previousTotalDelay)/countDelta;
			long countLong = currentTooLongCount - previousTooLongCount;
			
			logger.info("average:{} micro, time > {}ms: {}, max: {} micro", average/1000, tooLongBaseMilli, countLong,maxDelayNanos/1000);
			
			maxDelayNanos = 0;
			previousTotalCount = currentTotalCount;
			previousTotalDelay = currentTotalDelay;
			previousTooLongCount = currentTooLongCount;
		}
	}

}
