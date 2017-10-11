package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.task.ReshardingTask;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
	
	@Autowired
	private CurrentClusterServer currentClusterServer;
	
	private AtomicLong totalTasks = new AtomicLong(0);
	
	public static final String ARRANGE_TASK_EXECUTOR_START = "ArrangeTaskExecutorStart";
	
	private Thread taskThread;
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();

	}
	
	@Override
	protected void doStart() throws Exception {

		startTaskThread();
	}
	
	private void startTaskThread() {

		if(!Boolean.parseBoolean(System.getProperty(ARRANGE_TASK_EXECUTOR_START, "true"))){
			logger.info("[startTaskThread][system start false property]{}", ARRANGE_TASK_EXECUTOR_START);
			return;
		}

		if(shouldExit()){
			logger.info("[startTaskThread][should exit]");
			return;
		}

		if(taskThread == null){
			taskThread = XpipeThreadFactory.create(
					String.format("ArrangeTaskExecutor-(%d)", currentClusterServer.getServerId()) ).newThread(this);
			taskThread.start();
		}
	}


	@Override
	protected void doStop() throws Exception {
		if(taskThread != null){
			taskThread.interrupt();
		}
	}
	
	public void offer(ReshardingTask task){
		
		logger.info("[offer]{}", task);
		if(tasks.offer(task)){
			totalTasks.incrementAndGet();
		}else{
			logger.error("[offset][fail]{}", task);
		}
		
		startTaskThread();
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

	private boolean shouldExit() {
		
		if(getLifecycleState().isStarted() || getLifecycleState().isStarting()){
			return false;
		}
		return true;
	}

	@Override
	public void run() {
		
		while(!shouldExit()){
			try{
				executeTask();
			}catch(Throwable th){
				logger.error("[run]", th);
			}
		}
		logger.info("[run][break]");
		
		taskThread = null;
	}

	
	public long getTotalTasks() {
		return totalTasks.get();
	}

	private void executeTask() throws InterruptedException {
		
		ReshardingTask task = null;
		try{
			task = tasks.take();
			
			if(shouldExit()){
				logger.info("[executeTask][exit drop task]{}", task);
				return;
			}
			
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
			logger.info("[executeTask][ end ]{}", task);
			currentTask = null;
		}
	}

	public void setCurrentClusterServer(CurrentClusterServer currentClusterServer) {
		this.currentClusterServer = currentClusterServer;
	}

	protected Thread getTaskThread() {
		return taskThread;
	}
}
