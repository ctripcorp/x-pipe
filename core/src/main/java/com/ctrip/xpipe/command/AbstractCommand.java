package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public abstract class AbstractCommand<V> implements Command<V>{

	private Logger logger;

	protected AtomicReference<CommandFuture<V>> future = new AtomicReference<CommandFuture<V>>(new DefaultCommandFuture<>(this));

	@Override
	public CommandFuture<V> future() {
		if(future == null){
			return null;
		}
		return future.get();
	}
	
	@Override
	public CommandFuture<V> execute(){

		getLogger().debug("[execute]{}", this);
		return execute(MoreExecutors.directExecutor());
	}

	@Override
	public CommandFuture<V> execute(Executor executors) {

		if(future().isDone()){
			doExecuteWhenCommandDone();
		}
		
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
				}catch(Throwable th){
					if(!future().isDone()){
						future().setFailure(th);
					}else {
						getLogger().error("[execute][done, but exception]" + this, th);
					}
				}
			}
		});
		return future();
	}

	protected void doExecuteWhenCommandDone() {
		getLogger().info("[execute][already done, reset]{}, {}", this, future().getNow());
		reset();
	}
	
	
	protected void doCancel() {
		
	}

	protected abstract void doExecute() throws Throwable;

	protected void fail(Throwable ex) {
		
		future().setFailure(ExceptionUtils.getRootCause(ex));
	}
	
	
	@Override
	public void reset(){
		
		if(!future().isDone()){
			getLogger().info("[reset][not done]{}", this);
			future().cancel(true);
		}

		future.set(new DefaultCommandFuture<>(this));
		getLogger().info("[reset]{}", this);
		doReset();
	}
	
	protected abstract void doReset();

	@Override
	public String toString() {
		return String.format("CMD[%s]", getName());
	}

	protected Logger getLogger() {
		if(logger == null) {
			logger = LoggerFactory.getLogger(getClass());
		}
		return logger;
	}
	
}
