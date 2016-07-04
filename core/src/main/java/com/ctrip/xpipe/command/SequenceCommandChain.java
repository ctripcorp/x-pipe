package com.ctrip.xpipe.command;

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
	protected CommandFuture<Object> doExecute() throws CommandExecutionException {
		
		executeNext();
		return future;
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Command executeNext(){
		
		current.incrementAndGet();
		Command<?> command = getCommand(current.get());
		if(command == null){
			return null;
		}
		
		try {
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
		} catch (CommandExecutionException e) {
			logger.error("[executeNext]" + command, e);
			failExecuteNext(e);
		}
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
}
