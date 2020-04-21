package com.ctrip.xpipe.command;


import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.exception.XpipeException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public class CommandChainException extends XpipeException{

	private static final long serialVersionUID = 1L;
	
	private List<CommandFuture<?>> result;
	private String message;

	public CommandChainException(String message, List<CommandFuture<?>> result) {
		super(message);
		this.message = message;
		this.result = new LinkedList<>(result);
		
	}

	public CommandChainException(String message, Throwable th, List<CommandFuture<?>> result) {
		super(message, th);
		this.result = new LinkedList<>(result);
	}
	
	public List<CommandFuture<?>> getResult() {
		return result;
	}
	
	@Override
	public String getMessage() {
		
		StringBuilder sb = new StringBuilder();
		sb.append(message + ":");
		for(CommandFuture<?> future : result){
			if(!future.isSuccess()){
				sb.append(future.command() + ":" + future.cause().getMessage());
			}
		}
		return sb.toString();
	}

	@Override
	public synchronized Throwable getCause() {
		Throwable cause = super.getCause();
		if (null == cause) {
			ListIterator<CommandFuture<?> > reverseIterator = result.listIterator(result.size());

			// normally think that the last cause is reason for breaking chain execution
			while (reverseIterator.hasPrevious()) {
				CommandFuture<?> future = reverseIterator.previous();
				if (future.isDone() && !future.isSuccess()) return future.cause();
			}
		}

		return cause;
	}

}
