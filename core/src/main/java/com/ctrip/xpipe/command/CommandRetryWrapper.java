package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.retry.NoWaitRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Jul 14, 2016
 */
public final class CommandRetryWrapper<V> extends AbstractCommand<V>{
	
	protected Logger logger = LoggerFactory.getLogger(CommandRetryWrapper.class);
	
	private int retryTimes;
	private long timeoutTime;

	private RetryPolicy retryPolicy;
	private AtomicInteger executeCount = new AtomicInteger();
	
	private Command<V> command;
	private CommandFuture<V> currentCommandFuture;
	
	private ScheduledExecutorService scheduled;
	
	private CommandRetryWrapper(Command<V> command, ScheduledExecutorService scheduled){
		this(scheduled, 0, 0, new NoWaitRetry(), command);
	}

	public CommandRetryWrapper(ScheduledExecutorService scheduled, int retryTimes, int retryTimeoutMilli, RetryPolicy retryPolicy, Command<V> command) {
		
		this.scheduled = scheduled;
		this.retryTimes = retryTimes;
		this.retryPolicy = retryPolicy;
		this.command = command;
		if(retryTimeoutMilli >= 0){
			timeoutTime = retryTimeoutMilli + System.currentTimeMillis();
		}else{
			timeoutTime = retryTimeoutMilli;
		}
	}
	
	public static <V> Command<V>  buildCountRetry(int retryTimes, RetryPolicy retryPolicy, Command<V> command, ScheduledExecutorService scheduled){
		return new CommandRetryWrapper<>(scheduled, retryTimes, -1, retryPolicy, command);
	}
	
	public static <V> Command<V>  buildTimeoutRetry(int retryTimeoutMilli, RetryPolicy retryPolicy, Command<V> command, ScheduledExecutorService scheduled){
		return new CommandRetryWrapper<>(scheduled, -1, retryTimeoutMilli, retryPolicy, command);
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
					if(!shouldRetry(commandFuture.cause())){
						logger.info("[opetationComplete][retry fail than max retry]{}", command);
						future().setFailure(commandFuture.cause());
						return;
					}

					if(future().isDone()){
						logger.info("[future cancel][skip retry]{}, cause:{}", command, future().cause());
						return;
					}

					logger.error("[operationComplete]" + command, commandFuture.cause());
					
					int waitMilli = retryPolicy.retryWaitMilli();
					logger.info("[retry]{},{},{}", executeCount.get(), waitMilli, command);
					command.reset();
					execute(waitMilli, TimeUnit.MILLISECONDS);
				}
			}
		});
	}

	private void execute(int time, TimeUnit timeUnit) {
		new ScheduleCommandWrapper<>(this, scheduled, time, timeUnit).execute();
	}
	
	protected boolean shouldRetry(Throwable throwable) {
		
		if(retryTimes >=0 && executeCount.get() > retryTimes){
			logger.info("[shouldRetry][false][retry count]{} > {}", executeCount.get(), retryTimes);
			return false;
		}
		
		long current = System.currentTimeMillis();
		if(timeoutTime > 0 && current > timeoutTime){
			logger.info("[shouldRetry][false][retry timeout]{} > {}", current, timeoutTime);
			return false;
		}

		if(retryPolicy == null){
			return false;
		}

		if(!retryPolicy.retry(throwable)){
			logger.info("[shouldRetry][exception not retry]{}, {}", retryPolicy, throwable.getClass());
			return false;
		}
		
		return true;
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
