package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.retry.RetryDelay;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public class ParallelCommandChainTest extends AbstractCommandChainTest{
	
	private int totalCommandCount = 10;
	private int failIndex = 2;
	private String successMessage = randomString();


	@SuppressWarnings("unchecked")
	@Test
	public void testSuccess() throws InterruptedException, ExecutionException{

		Command<?>[] successCommands = createSuccessCommands(totalCommandCount, successMessage);
		ParallelCommandChain chain = new ParallelCommandChain(executors, successCommands);
		
		List<CommandFuture<?>> result = (List<CommandFuture<?>>) chain.execute().get();
		Assert.assertEquals(totalCommandCount, result.size());

		for(Command<?> success : successCommands){
			Assert.assertTrue(success.future().isSuccess());
		}
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

	@Test
	public void testCompleteAllCommands() throws Exception {
		AtomicBoolean result = new AtomicBoolean(false);
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		ParallelCommandChain chain = new ParallelCommandChain(executorService);
		int taskSize = 5;
		for(int i = 0; i < taskSize; i++) {
			chain.add(retry3Times(new TestCompleteCommand("success"+i, 100)));
		}
		chain.future().addListener(commandFuture -> {
			logger.info("{}", chain.getResult().size());
			result.getAndSet(taskSize == chain.getResult().size());
		});
		chain.execute().get();
		Assert.assertTrue(result.get());
	}

	private <T> Command<T> retry3Times(Command<T> command) {
		return new DefaultRetryCommandFactory(3, new RetryDelay(5), scheduled).createRetryCommand(command);
	}

	class TestCompleteCommand extends AbstractCommand<Void> {

		private String word;

		private long milli = 0;

		public TestCompleteCommand(String word) {
			this.word = word;
		}

		public TestCompleteCommand(String word, long milli) {
			this.word = word;
			this.milli = milli;
		}

		@Override
		protected void doExecute() throws Exception {
			Thread.sleep(milli);
			logger.info("[doExecute]{}", word);
			future().setSuccess();
		}

		@Override
		protected void doReset() {

		}

		@Override
		public String getName() {
			return null;
		}
	}

}
