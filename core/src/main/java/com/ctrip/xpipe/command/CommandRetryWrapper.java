package com.ctrip.xpipe.command;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.retry.NoWaitRetry;

/**
 * @author wenchao.meng
 *
 * Jul 14, 2016
 */
public class CommandRetryWrapper<V> extends AbstractCommand<V>{
	
	protected Logger logger = LoggerFactory.getLogger(CommandRetryWrapper.class);
	
	private int retryTimes;
	private long timeoutTime;

	private RetryPolicy retryWait;
	private AtomicInteger executeCount = new AtomicInteger();
	
	private Command<V> command;
	private CommandFuture<V> currentCommandFuture;
	
	public CommandRetryWrapper(Command<V> command){
		this(0, 0, new NoWaitRetry(), command);
	}

	public CommandRetryWrapper(int retryTimes, int retryTimeoutMilli, RetryPolicy retryWait, Command<V> command) {
		
		this.retryTimes = retryTimes;
		this.retryWait = retryWait;
		this.command = command;
		if(retryTimeoutMilli >= 0){
			timeoutTime = retryTimeoutMilli + System.currentTimeMillis();
		}else{
			timeoutTime = retryTimeoutMilli;
		}
	}
	
	public static <V> Command<V>  buildCountRetry(int retryTimes, RetryPolicy retryWait, Command<V> command){
		return new CommandRetryWrapper<>(retryTimes, -1, retryWait, command);
	}
	
	public static <V> Command<V>  buildTimeoutRetry(int retryTimeoutMilli, RetryPolicy retryWait, Command<V> command){
		return new CommandRetryWrapper<>(-1, retryTimeoutMilli, retryWait, command);
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
					future().setSuccess(commandFuture.get());
				}else{
					
					if(!shouldRetry()){
						logger.info("[opetationComplete][retry fail than max retry]{}", command);
						future().setFailure(commandFuture.cause());
						return;
					}

					if(future().isDone()){
						logger.info("[future cancel][skip retry]{}, cause:{}", command, future().cause());
						return;
					}

					logCause(commandFuture.cause());
					
					int waitMilli = retryWait.retryWaitMilli();
					logger.info("[retry]{}, {},{}", executeCount.get(), waitMilli, command);
					command.reset();
					execute(waitMilli, TimeUnit.MILLISECONDS);
				}
			}
		});
	}

	protected boolean shouldRetry() {
		
		if(retryTimes >=0 && executeCount.get() > retryTimes){
			logger.info("[shouldRetry][false][retry count]{} > {}", executeCount.get(), retryTimes);
			return false;
		}
		
		long current = System.currentTimeMillis();
		if(timeoutTime > 0 && current > timeoutTime){
			logger.info("[shouldRetry][false][retry timeout]{} > {}", current, timeoutTime);
			return false;
		}
		return true;
	}

	protected void logCause(Throwable cause) {
		ExceptionUtils.logException(logger, cause);
	}

	@Override
	protected void doReset(){
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
