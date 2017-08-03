package com.ctrip.xpipe.command;


import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;


/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public class DefaultCommandFutureTest extends AbstractTest{
	
	CommandFuture<Object> jobFuture = new DefaultCommandFuture<>(null);

	@Test
	public void testToString(){

		jobFuture.setSuccess();
		logger.info("{}", jobFuture);

		jobFuture = new DefaultCommandFuture<>(null);
		jobFuture.setFailure(new Exception("some exception"));
		logger.info("{}", jobFuture);

	}
	
	@Test
	public void testSuccess() throws InterruptedException, ExecutionException{
		
		final Object success = new Object();
		jobFuture.await(1000, TimeUnit.MILLISECONDS);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				jobFuture.setSuccess(success);
			}
		}).start();
		Assert.assertEquals(success, jobFuture.get());
		
	}

	@Test(expected = ExecutionException.class)
	public void testFailure() throws InterruptedException, ExecutionException{
		
		jobFuture.await(1000, TimeUnit.MILLISECONDS);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				jobFuture.setFailure(new Exception());
			}
		}).start();
		jobFuture.get();
		
	}

	@Test(expected = CancellationException.class)
	public void testCancel() throws InterruptedException, ExecutionException{
		
		jobFuture.await(1000, TimeUnit.MILLISECONDS);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				jobFuture.cancel(true);
			}
		}).start();
		
		sleep(100);
		Assert.assertTrue(jobFuture.isCancelled());
		jobFuture.get();
	}

}
