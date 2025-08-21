package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public class SequenceCommandChainTest extends AbstractCommandChainTest{
	
	private int totalCommandCount = 5;
	private int failIndex = 2;
	private String successMessage = randomString();
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSuccess() throws InterruptedException, ExecutionException{
		
		Command<?>[] commands = createSuccessCommands(totalCommandCount, successMessage);
		
		SequenceCommandChain chain = new SequenceCommandChain(commands);
		
		List<CommandFuture<?>> result = (List<CommandFuture<?>>) chain.execute().get();
		Assert.assertEquals(totalCommandCount, result.size());
		
	}

	@Test
	public void testCancel() throws InterruptedException, ExecutionException{
		
		final int sleepInterval = 1000;
		SequenceCommandChain chain = new SequenceCommandChain(true, createSuccessCommands(totalCommandCount, successMessage, sleepInterval));
		final CommandFuture<Object> future = chain.execute();
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				sleep(sleepInterval/2);
				future.cancel(true);
			}
		}).start();


		try{
			future.get();
		}catch(Exception e){
		}
		Assert.assertEquals(1, chain.executeCount());
		
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testFailContinue() throws InterruptedException, ExecutionException{
		
		SequenceCommandChain chain = new SequenceCommandChain(true, createCommands(totalCommandCount, successMessage, failIndex, new Exception("just throw")));
		List<CommandFuture<?>> result = (List<CommandFuture<?>>) chain.execute().get();
		Assert.assertEquals(totalCommandCount, result.size());
		
		for(int i=0;i<totalCommandCount;i++){
			if(i == failIndex){
				Assert.assertTrue(!result.get(i).isSuccess());
			}else{
				Assert.assertTrue(result.get(i).isSuccess());
			}
		}
	}

	@Test
	public void testFailStop() throws InterruptedException, ExecutionException{
		
		SequenceCommandChain chain = new SequenceCommandChain(false, createCommands(totalCommandCount, successMessage, failIndex, new Exception("just throw")));
		
		List<CommandFuture<?>> result = null;
		try{
			chain.execute().get();
			Assert.fail();
		}catch(ExecutionException e){
			Throwable th = e.getCause();
			if(th instanceof CommandChainException){
				result = ((CommandChainException) th).getResult();
			}else{
				Assert.fail();
			}
		}
		Assert.assertEquals(failIndex + 1, result.size());
		
		for(int i=0;i<=failIndex;i++){
			if(i == failIndex){
				Assert.assertTrue(!result.get(i).isSuccess());
			}else{
				Assert.assertTrue(result.get(i).isSuccess());
			}
		}
		
	}

	@Test(expected = ExecutionException.class)
	@Ignore
	public void testSeqCommandChainErrorLog() throws InterruptedException, ExecutionException {
		SequenceCommandChain chain = new SequenceCommandChain(this.getClass().getSimpleName());
		CommandFuture<String> f = new DefaultCommandFuture<>(new TestCommand("Success"));
		f.setFailure(new CommandTimeoutException("timeout 500"));
		for (Command<?> command : createCommands(totalCommandCount, successMessage, failIndex,
				new CommandChainException("outer exception", Collections.singletonList(f)))) {
			chain.add(command);
		}
		chain.execute().get();
	}

}
