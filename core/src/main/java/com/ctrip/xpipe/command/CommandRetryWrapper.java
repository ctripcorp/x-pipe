package com.ctrip.xpipe.command;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.retry.RetryWait;
import com.ctrip.xpipe.retry.NoWaitRetry;

/**
 * @author wenchao.meng
 *
 * Jul 14, 2016
 */
public class CommandRetryWrapper<V> extends AbstractCommand<V>{
	
	protected Logger logger = LoggerFactory.getLogger(CommandRetryWrapper.class);
	
	private int retryTimes;
	private RetryWait retryWait;
	private AtomicInteger executeCount = new AtomicInteger();
	
	private Command<V> command;
	private CommandFuture<V> currentCommandFuture;
	
	public CommandRetryWrapper(Command<V> command){
		this(0, new NoWaitRetry(), command);
	}
	
	public CommandRetryWrapper(int retryTimes, RetryWait retryWait, Command<V> command) {
		
		this.retryTimes = retryTimes;
		this.retryWait = retryWait;
		this.command = command;
	}
	
	@Override
	public String getName() {
		return "retry-" + command;
	}


	@Override
	protected void doExecute()  {
		
		executeCount.incrementAndGet();
		currentCommandFuture = command.execute();
		currentCommandFuture.addListener(new CommandFutureListener<V>() {

			@Override
			public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
				if(commandFuture.isSuccess()){
					future.setSuccess(commandFuture.get());
				}else{
					
					if(executeCount.get() > retryTimes){
						logger.info("[opetationComplete][retry fail than max retry]{}", command);
						future.setFailure(commandFuture.cause());
						return;
					}

					if(future.isDone()){
						logger.info("[future cancel][skip retry]{}, cause:{}", command, future.cause());
						return;
					}
					
					logger.error("[operationComplete]" + command, commandFuture.cause());
					
					int waitMilli = retryWait.retryWaitMilli();
					logger.info("[retry]{}, {},{}", executeCount.get(), waitMilli, command);
					command.reset();
					execute(waitMilli, TimeUnit.MILLISECONDS);
				}
			}
		});
	}

	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected void doCancel() {
		
		if(currentCommandFuture != null && !currentCommandFuture.isDone()){
			currentCommandFuture.cancel(true);
		}
	}

	public int getExecuteCount() {
		return executeCount.get();
	}
	
	
}
