package com.ctrip.xpipe.command;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandChain;
import com.ctrip.xpipe.api.command.CommandFuture;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public abstract class AbstractCommandChain extends AbstractCommand<Object> implements CommandChain<Object>{

	protected final List<Command<?>>  commands = new LinkedList<>();
	
	private final List<CommandFuture<?>> result = new LinkedList<>();
	
	private AtomicInteger current = new AtomicInteger(-1);

	public AbstractCommandChain() {
		
	}

	public AbstractCommandChain(@SuppressWarnings("rawtypes") List<Command> commandsList) {
		for(Command<?> command : commandsList){
			this.commands.add(command);
		}
	}

	public AbstractCommandChain(Command<?> ... commands) {
		for(Command<?> command : commands){
			this.commands.add(command);
		}
	}
	
	@Override
	protected void doExecute() throws Exception {
		
		if(commands.size() == 0){
			future().setSuccess();
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

	@Override
	public synchronized void add(Command<?> command){
		this.commands.add(command);
	}

	@Override
	public synchronized void remove(Command<?> command){
		this.commands.remove(command);
	}
	
	protected Command<?> getCommand(int index) {
		
		if(index >= commands.size()){
			return null;
		}
		return commands.get(index);
	}
	
	@Override
	protected void doReset(){
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
	
	protected Command<?> currentCommand(){
		return getCommand(current.get());
	}
	
	protected CommandFuture<?> executeNext(){

		int currentIndex = current.incrementAndGet();
		Command<?> command = getCommand(currentIndex);
		if(command == null){
			return null;
		}
		return executeCommand(command);
	}

	@Override
	public int executeCount() {
		return current.get() + 1;
	}

}
