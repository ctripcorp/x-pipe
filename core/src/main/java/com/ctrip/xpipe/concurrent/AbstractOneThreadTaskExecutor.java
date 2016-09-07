package com.ctrip.xpipe.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.retry.NoRetry;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
public abstract class AbstractOneThreadTaskExecutor extends AbstractExceptionLogTask{
	
	private Thread thread;
	
	private BlockingQueue<Command<?>> commands = new LinkedBlockingQueue<Command<?>>();
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public AbstractOneThreadTaskExecutor(){
		thread = new Thread(this);
		thread.setName(getClass().getSimpleName());
	}
	
	protected void startThreadIfPossible(){
		if(!thread.isAlive()){
			thread.start();
			logger.info("[startThreadIfPossible][start]");
		}
	}
	
	public Thread getThread() {
		return thread;
	}

	
	protected void putCommand(Command<?> command){
		
		commands.offer(command);
		startThreadIfPossible();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void doRun() {
		
		while(!shouldExit() && !Thread.currentThread().isInterrupted()){
			
			Command<?> command = commands.poll();
			if(command == null){
				break;
			}
			if(shouldExit()){
				logger.info("[doRun][should exit]");
				break;
			}
			
			RetryTemplate retry = getRetryTemplate();
			try {
				retry.execute(command);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			
		}
		
		logger.info("[doRun][break]");
	}

	@SuppressWarnings("rawtypes")
	protected RetryTemplate getRetryTemplate() {
		
		return new NoRetry<>();
	}

	protected boolean shouldExit(){
		return false;
	}
}
