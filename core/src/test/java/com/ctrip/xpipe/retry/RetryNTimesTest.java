package com.ctrip.xpipe.retry;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
	
	@Test
	public void testGetOriginalException() {
		assertTrue(RetryNTimes.getOriginalException(new IOException("test")) instanceof IOException);
		assertTrue(RetryNTimes.getOriginalException(
				new ExecutionException(new IllegalArgumentException("test"))) instanceof IllegalArgumentException);
		assertTrue(RetryNTimes.getOriginalException(new ExecutionException(null)) instanceof ExecutionException);
		assertTrue(RetryNTimes.getOriginalException(new ExecutionException(new InvocationTargetException(
				new HttpClientErrorException(HttpStatus.BAD_REQUEST, "test")))) instanceof HttpClientErrorException);
		assertTrue(RetryNTimes.getOriginalException(
				new ExecutionException(new InvocationTargetException(null))) instanceof InvocationTargetException);
		assertTrue(RetryNTimes.getOriginalException(new InvocationTargetException(new IOException("test"))) instanceof IOException);
		assertTrue(RetryNTimes.getOriginalException(new InvocationTargetException(null)) instanceof InvocationTargetException);

		assertFalse(RetryNTimes.getOriginalException(
				new InvocationTargetException(new IOException())) instanceof InvocationTargetException);
		assertFalse(RetryNTimes.getOriginalException(new ExecutionException(
				new InvocationTargetException(new IOException("test")))) instanceof ExecutionException);
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
