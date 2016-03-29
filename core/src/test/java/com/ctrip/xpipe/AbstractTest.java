package com.ctrip.xpipe;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.ctrip.xpipe.exception.DefaultExceptionHandler;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:44:47
 */
public class AbstractTest {
	
	protected Logger logger = LogManager.getLogger(getClass());
	
	protected ExecutorService executors = Executors.newCachedThreadPool();
	
	@Rule
	public TestName name = new TestName();
	
	@Before
	public void beforeAbstractTest(){
		
		Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());
		logger.info("[begin test]" + name.getMethodName());
	}


	protected void sleepSeconds(int seconds){
		sleep(seconds * 1000);
	}

	protected void sleep(int miliSeconds){
		
		try {
			TimeUnit.MILLISECONDS.sleep(miliSeconds);
		} catch (InterruptedException e) {
		}
	}
	
	
	@After
	public void afterAbstractTest(){
		
		logger.info("[end   test]" + name.getMethodName());
	}

	

}
