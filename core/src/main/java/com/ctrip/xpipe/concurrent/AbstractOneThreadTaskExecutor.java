package com.ctrip.xpipe.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

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
	
	private AtomicReference<Thread> thread = new AtomicReference<>(null);
	
	private BlockingQueue<Command<?>> commands = new LinkedBlockingQueue<Command<?>>();
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public AbstractOneThreadTaskExecutor(){
	}
	
	protected synchronized void startThreadIfPossible(){

		if(thread.get() == null && commands.size() > 0){
			Thread curThread = new Thread(this);
			curThread.setName(getClass().getSimpleName());
			thread.set(curThread);
			curThread.start();
			logger.info("[startThreadIfPossible][start]");
		}
	}
	
	
	
	public Thread getThread() {
		return thread.get();
	}

	
	protected void putCommand(Command<?> command){
		
		commands.offer(command);
		startThreadIfPossible();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void doRun() {

		try{
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
		}finally{
			logger.info("[doRun][break]");
			thread.set(null);
			startThreadIfPossible();
		}
	}

	@SuppressWarnings("rawtypes")
	protected RetryTemplate getRetryTemplate() {
		
		return new NoRetry<>();
	}

	protected boolean shouldExit(){
		return false;
	}
}
