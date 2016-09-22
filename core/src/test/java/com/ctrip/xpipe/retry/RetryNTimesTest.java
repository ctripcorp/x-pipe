package com.ctrip.xpipe.retry;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
public class RetryNTimesTest extends AbstractTest{
	
	@Test
	public void testSuccess() throws Exception{
		
		RetryNTimes<Object> retryNTimes = new RetryNTimes<Object>(100, new RetryDelay(100));
		Assert.assertNotNull(retryNTimes.execute(new TestRetryCommand(new Object())));
	}
	
	@Test
	public void testFailRetry() throws Exception{

		int retryTimes = 3;
		RetryDelay retryDelay = new RetryDelay(100);
		RetryNTimes<Object> retryNTimes = new RetryNTimes<Object>(retryTimes, retryDelay);
		try {
			retryNTimes.execute(new TestRetryCommand(new Exception("just fail")));
		} catch (Exception e) {
			Assert.assertEquals(3, retryDelay.getRetryTimes());
			Assert.assertEquals("just fail", e.getMessage());
		}

	}
	
	@Test
	public void testFailNoRetry() throws Exception{

		RetryPolicy retryPolicy = new RetryExceptionIsSuccess(100);
		RetryNTimes<Object> retryNTimes = new RetryNTimes<Object>(3, retryPolicy);
		try {
			retryNTimes.execute(new TestRetryCommand(new Exception("just fail")));
		} catch (Exception e) {
			Assert.assertEquals(0, retryPolicy.getRetryTimes());
			Assert.assertEquals("just fail", e.getMessage());
		}

	}
	
	public class RetryExceptionIsSuccess extends RetryDelay{
		public RetryExceptionIsSuccess(int delayBaseMilli) {
			super(delayBaseMilli);
		}

		@Override
		public boolean retry(Throwable th) {
			return false;
		}
	}
	
	public static class TestRetryCommand extends AbstractCommand<Object>{

		private Object result;
		public TestRetryCommand(Object result) {
			this.result = result;
		}

		@Override
		public String getName() {
			return "test_retry";
		}

		@Override
		protected void doExecute() throws Exception {
			
			if(result instanceof Exception){
				throw (Exception)result; 
			}
			
			future().setSuccess(result);
		}

		@Override
		protected void doReset(){
			
		}
	}
}
