package com.ctrip.xpipe.command;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public abstract class AbstractCommandChain extends AbstractCommand<Object>{
	
	private final List<Command<?>>  commands = new LinkedList<>();
	
	public AbstractCommandChain(Command<?> ... commands) {
		
		for(Command<?> command : commands){
			this.commands.add(command);
		}
	}
	
	@Override
	public String getName() {
		StringBuilder sb = new StringBuilder();
		for(Command<?> command : commands){
			sb.append(command.getName());
			sb.append(" ");
		}
		return sb.toString();
	}

	protected void add(Command<?> command){
		this.commands.add(command);
	}

	protected void remove(Command<?> command){
		this.commands.remove(command);
	}
	
	protected Command<?> getCommand(int index) {
		
		if(index >= commands.size()){
			return null;
		}
		return commands.get(index);
	}
	
	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		for(Command<?> command : commands){
			command.reset();
		}
	}

}
