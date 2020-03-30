package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public class ParallelCommandChain extends AbstractCommandChain{
	
	private Executor executors;
	private List<CommandFuture<?>> completed = new LinkedList<>();

	private boolean isLoggable = true;

	public ParallelCommandChain(Executor executors){
		this.executors = executors;
		if(this.executors == null){
			this.executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("ParallelCommandChain"));
		}
	}

	public ParallelCommandChain(Executor executors, boolean loggable){
		this.executors = executors;
		if(this.executors == null){
			this.executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("ParallelCommandChain"));
		}
		isLoggable = loggable;
	}

	public ParallelCommandChain(Command<?> ...commands) {
		this(null, commands);
	}

	public ParallelCommandChain(ExecutorService executors, Command<?> ...commands) {
		super(commands);
		if(executors == null){
			executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("ParallelCommandChain"));
		}
		this.executors = executors;
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void doExecute() throws Exception {
		super.doExecute();
		
		for(int i=0; i < commands.size() ;i++){
			executors.execute(new Runnable() {
				@Override
				public void run() {
					CommandFuture<?> future = executeNext();
					future.addListener(new CommandFutureListener() {

						@Override
						public void operationComplete(CommandFuture commandFuture) throws Exception {
							addComplete(commandFuture);
						}
					});

				}
			});
		}
	}

	private void addComplete(CommandFuture<?> commandFuture) {
		
		
		synchronized (completed) {
			completed.add(commandFuture);
		}
		
		if(future().isCancelled()){
			return;
		}
		
		if(completed.size() >= commands.size()){
			if(isLoggable) {
				getLogger().info("[addComplete][all complete]{}, {}", completed.size(), getResult().size());
			}
			boolean fail = false;
			for(CommandFuture<?> future : completed){
				if(!future.isSuccess()){
					fail = true;
					break;
				}
			}
			synchronized (this) {
				if(!future().isDone()){
					if(!fail){
						future().setSuccess(getResult());
					}else{
						future().setFailure(new CommandChainException("parallel fail", getResult()));
					}
				}
			}
		}
	}
	
	@Override
	protected void doCancel() {
		
		List<CommandFuture<?>> executed = new LinkedList<>(getResult());
		executed.removeAll(completed);
		for(CommandFuture<?> future : executed){
			future.cancel(true);
		}
		super.doCancel();
	}
}
