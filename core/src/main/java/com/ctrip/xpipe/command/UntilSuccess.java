package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Mar 6, 2017
 */
public class UntilSuccess extends AbstractCommandChain{

	public UntilSuccess() {
	}

	public UntilSuccess(@SuppressWarnings("rawtypes") List<Command> commandsList) {
		super(commandsList);
	}

	public UntilSuccess(Command<?> ... commands) {
		super(commands);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void doExecute() throws Exception {
		
		if(future().isCancelled()){
			return;
		}

		CommandFuture<?> future = executeNext();
		if(future == null){
			future().setFailure(new CommandChainException("until success fail", getResult()));
			return;
		}

		future.addListener(new CommandFutureListener() {
			@Override
			public void operationComplete(CommandFuture commandFuture) throws Exception {

				if(commandFuture.isSuccess()){
					future().setSuccess(commandFuture.get());
				}else{
					logger.error("[doExecute]" + currentCommand(), commandFuture.cause());
					doExecute();
				}
			}
		});
	}
	
	@Override
	protected void doCancel() {
		super.doCancel();
		
		Command<?> currentCommand = currentCommand();
		if(currentCommand != null){
			currentCommand.future().cancel(true);
		}
		
	}
}
