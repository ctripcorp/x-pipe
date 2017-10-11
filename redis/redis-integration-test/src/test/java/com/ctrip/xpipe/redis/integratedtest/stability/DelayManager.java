package com.ctrip.xpipe.redis.integratedtest.stability;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *
 * Nov 25, 2016
 */
public class DelayManager{
	
	private static Logger logger = LoggerFactory.getLogger(DelayManager.class);

	private String desc;
	private int delayPrintInterval = Integer.parseInt(System.getProperty("delay-print-interval", "5000"));
	
	private ScheduledExecutorService scheduled;
	
	private int tooLongBaseMilli;
	
	private AtomicLong tooLongCount = new AtomicLong();
	
	private long maxDelayNanos = 0;
	private long maxDelayTime = 0;
	
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
    private boolean info = true;
	
	private AtomicLong totalCount = new AtomicLong();
	private AtomicLong totalDelay = new AtomicLong();

	public DelayManager(ScheduledExecutorService scheduled, String desc, int tooLongBaseMilli, boolean info){
		this.desc = desc;
		this.scheduled = scheduled;
		this.tooLongBaseMilli = tooLongBaseMilli;
		this.info = info;
		this.scheduled.scheduleAtFixedRate(new DelayPrinter(), delayPrintInterval, delayPrintInterval, TimeUnit.MILLISECONDS);
	}

	public DelayManager(ScheduledExecutorService scheduled, String desc, int tooLongBaseMilli){
		this(scheduled, desc, tooLongBaseMilli, true);
	}
	
	public void delay(long delayNanos){
		
		totalCount.incrementAndGet();
		totalDelay.addAndGet(delayNanos);
		
		if(delayNanos >= tooLongBaseMilli * 1000000){
			
			logger.debug("{}", delayNanos);
			tooLongCount.incrementAndGet();
		}
		
		if(delayNanos > maxDelayNanos){
			maxDelayNanos = delayNanos;
			maxDelayTime = System.currentTimeMillis();
			
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
			
			if(info){
				logger.info("{}, average:{} micro, time > {}ms: {}", desc, average/1000, tooLongBaseMilli, countLong);
				logger.info("{}, max: {} micro, happen time:{}", desc, maxDelayNanos/1000, sdf.format(new Date(maxDelayTime)));
			}else{
				logger.debug("{}, average:{} micro, time > {}ms: {}", desc, average/1000, tooLongBaseMilli, countLong);
				logger.debug("{}, max: {} micro, happen time:{}", desc, maxDelayNanos/1000, sdf.format(new Date(maxDelayTime)));
			}
			
			maxDelayNanos = 0;
			previousTotalCount = currentTotalCount;
			previousTotalDelay = currentTotalDelay;
			previousTooLongCount = currentTooLongCount;
		}
	}

}
