package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.exception.ExceptionUtils;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class SequenceCommandChain extends AbstractCommandChain{
	
	private boolean failContinue = false;

	private boolean isLoggable = true;

	public SequenceCommandChain(String tag) {
		super(tag);
	}

	public SequenceCommandChain(boolean failContinue){
		this.failContinue = failContinue;
	}

	public SequenceCommandChain(boolean failContinue, boolean isLoggable){
		this.failContinue = failContinue;
		this.isLoggable = isLoggable;
	}
	
	public SequenceCommandChain(Command<?> ... commands) {
		this(false, commands);
	}

	public SequenceCommandChain(boolean failContinue, Command<?> ... commands) {
		super(commands);
		this.failContinue = failContinue;
	}
	@Override
	protected void doExecute() throws Exception {
		super.doExecute();
		
		executeChain();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void executeChain(){
		
		if(future().isCancelled()){
			return;
		}
		
		CommandFuture<?> currentFuture = executeNext();
		if(currentFuture == null){
			future().setSuccess(getResult());
			return;
		}
		
		currentFuture.addListener(new CommandFutureListener() {

			@Override
			public void operationComplete(CommandFuture commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					executeChain();
				}else{
					failExecuteNext(commandFuture);
				}
			}
		});
	}

	private void failExecuteNext(CommandFuture<?> commandFuture) {
		logFail(commandFuture);
		
		if(failContinue){
			executeChain();
			return;
		}
		
		future().setFailure(new CommandChainException("sequence chain, fail stop", commandFuture.cause(), getResult()));
	}

	private void logFail(CommandFuture<?> commandFuture) {
		if (!isLoggable)
			return;
		if (ExceptionUtils.isStackTraceUnnecessary(commandFuture.cause())) {
			getLogger().error("[{}][failExecuteNext]{}, {}", tag, commandFuture.command(), commandFuture.cause().getMessage());
		} else {
			getLogger().error("[{}][failExecuteNext]{}", tag, commandFuture.command(), commandFuture.cause());
		}
	}

}
