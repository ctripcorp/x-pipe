package com.ctrip.xpipe.command;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

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
	
	private AtomicInteger current = new AtomicInteger(-1);
	
	public SequenceCommandChain(Command<?> ... commands) {
		this(false, commands);
	}

	public SequenceCommandChain(boolean failContinue, Command<?> ... commands) {
		super(commands);
		this.failContinue = failContinue;
	}

	@Override
	protected void doExecute() throws CommandExecutionException {
		
		executeNext();
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Command executeNext(){
		
		current.incrementAndGet();
		Command<?> command = getCommand(current.get());
		if(command == null){
			return null;
		}
		
		command.execute().addListener(new CommandFutureListener() {

			@Override
			public void operationComplete(CommandFuture commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					Command next = executeNext();
					if(next == null){
						future.setSuccess(commandFuture.get());
					}
				}else{
					failExecuteNext(commandFuture.cause());
				}
			}
		});
		return command;
	}

	private void failExecuteNext(Throwable throwable) {

		if(failContinue){
			Command<?> next = executeNext();
			if(next == null){
				future.setFailure(throwable);
			}
			return;
		}
		
		future.setFailure(throwable);
	}

	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		
		super.doReset();
		current.set(-1);
	}
}
