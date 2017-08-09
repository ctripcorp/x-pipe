package com.ctrip.xpipe.command;


import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.exception.XpipeException;

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

}
