package com.ctrip.xpipe.redis.integratedtest.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Jan 9, 2017
 */
public class SimpleLockTest {
	
	private ExecutorService executors = Executors.newCachedThreadPool();
	
	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4);
	
	private int checkIntervalSeconds = Integer.parseInt(System.getProperty("checkInterval", "600"));
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private long lock1StartTime;
	
	private long lock2StartTime;
	
	public static void main(String []argc){
		
		new SimpleLockTest().start();
	}
	
	private void start() {
		scheduled.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				startTest();
			}
		}, 0, checkIntervalSeconds, TimeUnit.SECONDS);
	}

	protected void startTest() {
		
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				
				logger.info("[begin][lock1]");
				lock1StartTime = System.currentTimeMillis();
				lock1();
				logger.info("[ end ][lock1]");
			}
		});

		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				
				logger.info("[begin][lock2]");
				lock2StartTime = System.currentTimeMillis();
				lock2();
				logger.info("[ end ][lock2]");
			}
		});
	}

	protected synchronized void lock1(){
		long current = System.currentTimeMillis();
		long duration = current - lock1StartTime ; 
		if( duration > 10){
			logger.info("[in][lock1]{}", duration);
		}
	}
	
	protected synchronized void lock2(){
		
		long current = System.currentTimeMillis();
		long duration = current - lock2StartTime ;
		if(duration > 10){
			logger.info("[in][lock2]{}", duration);
		}
	}
}
