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
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
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
		
		logger.debug("[execute]{}", this);
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
				}catch(Exception e){
					if(!future().isDone()){
						future().setFailure(e);
					}else {
						logger.error("[execute][done, but exception]" + this, e);
					}
				}
			}
		});
		return future();
	}

	protected void doExecuteWhenCommandDone() {
		logger.info("[execute][already done, reset]{}, {}", this, future().getNow());
		reset();
	}
	
	
	protected void doCancel() {
		
	}

	protected abstract void doExecute() throws Exception;

	protected void fail(Throwable ex) {
		
		future().setFailure(ExceptionUtils.getRootCause(ex));
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
		return String.format("CMD[%s]", getName());
	}
	
}
