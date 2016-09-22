package com.ctrip.xpipe.concurrent;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.Destroyable;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.retry.NoRetry;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
public class OneThreadTaskExecutor implements Destroyable{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private ExecutorService executors;
	
	@SuppressWarnings("rawtypes")
	private RetryTemplate retryTemplate;
	
	public OneThreadTaskExecutor(String threadName){
		this(new NoRetry<>(), threadName);
	}
	
	public OneThreadTaskExecutor(RetryTemplate<?> retryTemplate, String threadName){
		this.retryTemplate = retryTemplate;
		executors = Executors.newSingleThreadExecutor(XpipeThreadFactory.create(threadName));
	}
		
	public void executeCommand(Command<?> command){
		executors.execute(new Task(command));
	}
	
	
	public class Task extends AbstractExceptionLogTask{
		
		private Command<?> command;
		public Task(Command<?> command){
			this.command = command;
		}

		@Override
		@SuppressWarnings({ "unchecked" })
		protected void doRun() throws Throwable {
			retryTemplate.execute(command);
		}
		
	}
	
	@Override
	public void destroy() throws Exception {
		
		List<Runnable> tasks = executors.shutdownNow();
		if(tasks.size() > 0){
			logger.info("[destroy][un execute tasks]{}", tasks);
		}
	}
}
