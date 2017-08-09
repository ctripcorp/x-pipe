package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class SequenceCommandChain extends AbstractCommandChain{
	
	private boolean failContinue = false;

	public SequenceCommandChain(boolean failContinue){
		this.failContinue = failContinue;
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
		
		logger.error("[failExecuteNext]" + commandFuture.command(), commandFuture.cause());
		
		if(failContinue){
			executeChain();
			return;
		}
		
		future().setFailure(new CommandChainException("sequence chain, fail stop", getResult()));
	}

}
