package com.ctrip.xpipe.command;


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

	public CommandChainException(String message, List<CommandFuture<?>> result) {
		super(message);
		this.result = result;
		
	}

	public CommandChainException(String message, Throwable th, List<CommandFuture<?>> result) {
		super(message, th);
		this.result = result;
	}
	
	public List<CommandFuture<?>> getResult() {
		return result;
	}

}
