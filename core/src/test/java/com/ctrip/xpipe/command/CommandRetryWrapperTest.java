package com.ctrip.xpipe.command;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.retry.RetryDelay;

/**
 * @author wenchao.meng
 *
 * Jul 14, 2016
 */
public class CommandRetryWrapperTest extends AbstractTest{

	private int retryCount = 3;
	private String message = randomString();
	private int sleepBase = 100;

	@Test
	public void testSucccess() throws InterruptedException, ExecutionException{

		CommandRetryWrapperTestCommand command = new CommandRetryWrapperTestCommand(message);
		CommandRetryWrapper<String> wrapper = new CommandRetryWrapper<>(retryCount, new RetryDelay(sleepBase), command);
		
		Assert.assertEquals(message, wrapper.execute().get());;
	}
	
	@Test
	public void testRetry() throws CommandExecutionException{
		
		CommandRetryWrapperTestCommand command = new CommandRetryWrapperTestCommand(new Exception("just throw"));
		CommandRetryWrapper<String> wrapper = new CommandRetryWrapper<>(retryCount, new RetryDelay(sleepBase), command);
		try {
			wrapper.execute().get();
			Assert.fail();
		} catch (InterruptedException | ExecutionException e) {
		}
		
		Assert.assertEquals(wrapper.getExecuteCount(), retryCount + 1);
		
	}
	
	@Test
	public void testRetryCancel(){

		CommandRetryWrapperTestCommand command = new CommandRetryWrapperTestCommand(new Exception("just throw"));
		CommandRetryWrapper<String> wrapper = new CommandRetryWrapper<>(retryCount, new RetryDelay(sleepBase), command);
		
		final CommandFuture<String> future = wrapper.execute();
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				sleep(sleepBase);
				future.cancel(true);
			}
		}).start();

		try {
			future.get();
			Assert.fail();
		} catch (Exception e) {
		}
		
		Assert.assertTrue(wrapper.getExecuteCount() < (retryCount + 1));
		
	}
	
	
	public class CommandRetryWrapperTestCommand extends AbstractCommand<String>{
		
		private Exception e;
		private String successMessage;

		public CommandRetryWrapperTestCommand(String successMessage) {
			this(null, successMessage);
		}

		public CommandRetryWrapperTestCommand(Exception e) {
			this(e, "OK");
		}

		public CommandRetryWrapperTestCommand(Exception e, String successMessage) {
			this.e = e;
			this.successMessage = successMessage;
		}

		@Override
		public String getName() {
			return "CommandRetryWrapperTest";
		}

		@Override
		protected void doExecute() throws Exception {
			
			try {
				TimeUnit.MILLISECONDS.sleep(100);
				if(e != null){
					throw new CommandExecutionException("error", e);
				}
				future.setSuccess(successMessage);
			} catch (InterruptedException e) {
			}
		}

		@Override
		protected void doReset() throws InterruptedException, ExecutionException {
			
		}
		
	}

}
