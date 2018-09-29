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
 * Jan 16, 2017
 */
public class FailSafeCommandWrapperTest extends AbstractTest{
	
	@Test
	public void testSuccess() throws InterruptedException, ExecutionException, TimeoutException{
		
		String successMessage = randomString();
		
		FailSafeCommandWrapper<String> command = new FailSafeCommandWrapper<>(new TestCommand(successMessage));
		
		Assert.assertEquals(successMessage, command.execute().get(1, TimeUnit.SECONDS));
		
	}
	
	@Test
	public void testFail() throws InterruptedException, ExecutionException, TimeoutException{

		FailSafeCommandWrapper<String> command = new FailSafeCommandWrapper<>(new TestCommand(new Exception("just fail")));
		
		command.execute().get(1, TimeUnit.SECONDS);
	}
	
	@Test
	public void testCancel(){

		TestCommand testCommand = new TestCommand(randomString(), 1000);
		
		FailSafeCommandWrapper<String> command = new FailSafeCommandWrapper<>(testCommand);
		
		CommandFuture<String> future = command.execute();

		sleep(10);
		future.cancel(true);
		try {
			testCommand.future().get();
			Assert.fail();
		} catch(CancellationException e){
		}catch (InterruptedException | ExecutionException e) {
			Assert.fail();
		}
		
	}
}
