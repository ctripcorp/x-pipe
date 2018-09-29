package com.ctrip.xpipe.command;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *
 * Dec 21, 2016
 */
public class ScheduleCommandWrapperTest extends AbstractTest{
	
	private int executeTime = 100;
	private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
	
	@Test
	public void testSuccess() throws InterruptedException, ExecutionException, TimeoutException{

		String success = randomString();

		TestCommand testCommand = new TestCommand(success);
		
		CommandFuture<String> future = new ScheduleCommandWrapper<String>(testCommand, scheduled, executeTime, timeUnit).execute();

		sleep(executeTime/10);
		String result = future.getNow();
		Assert.assertNull(result);

		result = future.get(executeTime*10, TimeUnit.MILLISECONDS);
		Assert.assertEquals(success, result);

	}

	@Test
	public void testFailure(){

		TestCommand testCommand = new TestCommand(new Exception());
		
		CommandFuture<String> future = new ScheduleCommandWrapper<String>(testCommand, scheduled, executeTime, timeUnit).execute();

		try {
			future.get(executeTime*10, timeUnit);
			Assert.fail();
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
		}

	}

	@Test(expected = CancellationException.class)
	public void testCancel() throws InterruptedException, ExecutionException, TimeoutException{
		
		String request = randomString() + "\r\n";

		TestCommand testCommand = new TestCommand(request);
		CommandFuture<String> future = new ScheduleCommandWrapper<String>(testCommand, scheduled, executeTime, timeUnit).execute();

		sleep(10);
		String result = future.getNow();
		Assert.assertNull(result);

		future.cancel(false);
		
		try{
			testCommand.future().get();
			Assert.fail();
		}catch(Exception e){
			Assert.assertTrue(e instanceof CancellationException);
		}
		
		result = future.get(executeTime * 10, timeUnit);
	}
	
}
