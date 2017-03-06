package com.ctrip.xpipe.command;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.api.command.CommandFuture;

/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public class ParallelCommandChainTest extends AbstractCommandChainTest{
	
	private int totalCommandCount = 5;
	private int failIndex = 2;
	private String successMessage = randomString();

	
	@SuppressWarnings("unchecked")
	@Test
	public void testSuccess() throws InterruptedException, ExecutionException{
		
		ParallelCommandChain chain = new ParallelCommandChain(executors, createSuccessCommands(totalCommandCount, successMessage));
		
		List<CommandFuture<?>> result = (List<CommandFuture<?>>) chain.execute().get();
		Assert.assertEquals(totalCommandCount, result.size());
	}
	
	@Test
	public void testFail() throws InterruptedException, ExecutionException{
		
		ParallelCommandChain chain = new ParallelCommandChain(executors, createCommands(totalCommandCount, successMessage, failIndex, new Exception("just throw")));
		
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
		
		Assert.assertEquals(totalCommandCount, result.size());
		int successCount = 0;
		for(int i=0;i<totalCommandCount;i++){
			if(result.get(i).isSuccess()){
				successCount++;
			}
		}
		
		Assert.assertEquals(totalCommandCount - 1, successCount);
	}
	
	@Test
	public void testCancel() throws InterruptedException, ExecutionException{

		final int sleepInterval = 1000;
		ParallelCommandChain chain = new ParallelCommandChain(executors, createSuccessCommands(totalCommandCount, successMessage, sleepInterval));
		final CommandFuture<Object> future = chain.execute();
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				sleep(sleepInterval/2);
				future.cancel(true);
				
			}
		}).start();
		
		sleep((int) (sleepInterval * 1.5));
		
		Assert.assertEquals(totalCommandCount, chain.getResult().size());
		
		for(CommandFuture<?> current : chain.getResult()){
			Assert.assertTrue(current.isCancelled());
		}
	}


}
