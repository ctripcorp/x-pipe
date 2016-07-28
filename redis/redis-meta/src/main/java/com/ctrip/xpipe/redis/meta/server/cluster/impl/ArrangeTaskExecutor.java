package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ReshardingTask;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author wenchao.meng
 *
 * Jul 27, 2016
 */
@Component
public class ArrangeTaskExecutor extends AbstractLifecycle implements TopElement, Runnable{
	
	private BlockingQueue<ReshardingTask> tasks = new LinkedBlockingQueue<>();
	private int waitTaskTimeoutMilli =  60000; 
	private CommandFuture<Void> currentTask = null;
	private AtomicLong totalTasks = new AtomicLong(0);
	
	public static final String ARRANGE_TASK_EXECUTOR_START = "ArrangeTaskExecutorStart";
	
	@Override
	protected void doStart() throws Exception {
		
		if(Boolean.parseBoolean(System.getProperty(ARRANGE_TASK_EXECUTOR_START, "true"))){
			XpipeThreadFactory.create("ArrangeTaskExecutor").newThread(this).start();
		}
	}
	
	
	@Override
	protected void doStop() throws Exception {
	}
	
	public void offer(ReshardingTask task){
		logger.info("[offer]{}", task);
		if(tasks.offer(task)){
			totalTasks.incrementAndGet();
		}else{
			logger.error("[offset][fail]{}", task);
		}
	}
	
	public void clearTasks(){
		
		logger.info("[clearTasks]{}", tasks);
		tasks.clear();
		
		if(currentTask != null){
			currentTask.cancel(true);
		}
	}

	@Override
	public int getOrder() {
		return 0;
	}

	@Override
	public void run() {
		
		while(getLifecycleState().isStarted() || getLifecycleState().isStarting()){
			try{
				executeTask();
			}catch(Throwable th){
				logger.error("");
			}
		}
		logger.info("[run][break]");
	}

	
	public long getTotalTasks() {
		return totalTasks.get();
	}

	private void executeTask() throws InterruptedException {
		try{
			ReshardingTask task = tasks.take();
			logger.info("[executeTask][begin]{}", task);
			currentTask = task.execute();
			if(!currentTask.await(waitTaskTimeoutMilli, TimeUnit.MILLISECONDS)){
				logger.info("[executeTask][task timeout]{}", waitTaskTimeoutMilli);
				currentTask.cancel(true);
				return;
			}
			if(!currentTask.isSuccess()){
				logger.error("[executTask][fail]" + task, currentTask.cause());
			}
		}finally{
			currentTask = null;
		}
	}
}
