package com.ctrip.xpipe.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;

/**
 * @author wenchao.meng
 *
 * Jan 16, 2017
 */
public class FailSafeCommandWrapper<V> extends AbstractCommand<V>{

	private Command<V> command;
	
	public FailSafeCommandWrapper(Command<V> command) {
		this.command = command;
	}

	@Override
	public String getName() {
		return command.getName();
	}

	@Override
	protected void doExecute() throws Exception {
		
		command.execute().addListener(new CommandFutureListener<V>() {

			@Override
			public void operationComplete(CommandFuture<V> commandFuture) throws Exception {
				
				if(commandFuture.isSuccess()){
					future().setSuccess(commandFuture.get());
				}else{
					if(!future().isDone()){
						getLogger().error("[command fail, but treat it as success]" + command, commandFuture.cause());
						future().setSuccess();
					}
				}
			}
		});
		
	}

	@Override
	protected void doReset() {
		command.reset();
	}
	
	@Override
	protected void doCancel() {
		super.doCancel();
		
		CommandFuture<V> future = command.future();
		if(!future.isDone()){
			future.cancel(true);
		}
	}
}
