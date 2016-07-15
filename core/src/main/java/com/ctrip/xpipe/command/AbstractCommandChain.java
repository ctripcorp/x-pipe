package com.ctrip.xpipe.command;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public abstract class AbstractCommandChain extends AbstractCommand<List<CommandFuture<?>>>{
	
	protected final List<Command<?>>  commands = new LinkedList<>();
	
	private final List<CommandFuture<?>> result = new LinkedList<>();
	
	private AtomicInteger current = new AtomicInteger(-1);

	
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

	protected synchronized void add(Command<?> command){
		this.commands.add(command);
	}

	protected synchronized void remove(Command<?> command){
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
		current.set(-1);
	}
	
	public void addResult(CommandFuture<?> future){
		result.add(future);
	}
	
	public List<CommandFuture<?>> getResult() {
		return result;
	}
	
	public CommandFuture<?> executeCommand(Command<?> command){
		
		CommandFuture<?> future = command.execute();
		addResult(future);
		return future;
	}
	
	
	protected CommandFuture<?> executeNext(){
		
		current.incrementAndGet();
		Command<?> command = getCommand(current.get());
		if(command == null){
			return null;
		}
		return executeCommand(command);
	}
	
	public int getExecuteCount() {
		return current.get() + 1;
	}

}
