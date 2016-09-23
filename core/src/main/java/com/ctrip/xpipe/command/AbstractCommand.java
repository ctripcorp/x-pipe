package com.ctrip.xpipe.command;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public abstract class AbstractCommand<V> implements Command<V>{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ScheduledExecutorService scheduled;
	
	protected AtomicReference<CommandFuture<V>> future = new AtomicReference<CommandFuture<V>>(new DefaultCommandFuture<>(this));

	public AbstractCommand(ScheduledExecutorService scheduled) {
		this.scheduled = scheduled;
	}

	public AbstractCommand() {
		this.scheduled = Executors.newScheduledThreadPool(OsUtils.getCpuCount(), XpipeThreadFactory.create(getClass().getName()));
	}

	@Override
	public CommandFuture<V> future() {
		return future.get();
	}
	
	@Override
	public CommandFuture<V> execute(){
		return execute(MoreExecutors.sameThreadExecutor());
	}

	@Override
	public CommandFuture<V> execute(ExecutorService executors) {
		
		future().addListener(new CommandFutureListener<V>() {

			@Override
			public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
				if(commandFuture.isCancelled()){
					doCancel();
				}
			}
		});
		
		executors.execute(new Runnable() {
			
			@Override
			public void run() {
				try{
					doExecute();
				}catch(Exception e){
					if(!future().isDone()){
						future().setFailure(e);
					}
				}
			}
		});
		return future();
	}
	
	
	protected void doCancel() {
		
	}

	protected abstract void doExecute() throws Exception;

	@Override
	public CommandFuture<V> execute(final int time, TimeUnit timeUnit) {
		
		final ScheduledFuture<?> scheduleFuture = scheduled.schedule(new Runnable() {
			
			@Override
			public void run() {
				execute();
			}
		}, time, timeUnit);

		future().addListener(new CommandFutureListener<V>() {

			@Override
			public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
				if(commandFuture.isCancelled()){
					logger.info("[command canceled][cancel execution]{}", time);
					scheduleFuture.cancel(false);
				}
			}
		});
		return future.get();
	}
	
	@Override
	public void reset(){
		
		if(!future().isDone()){
			logger.info("[reset][not done]{}", this);
			future().cancel(true);
		}

		future.set(new DefaultCommandFuture<>(this));
		logger.info("[reset]{}", this);
		doReset();
	}
	
	protected abstract void doReset();

	@Override
	public String toString() {
		return "Command:" + getName();
	}
	
}
