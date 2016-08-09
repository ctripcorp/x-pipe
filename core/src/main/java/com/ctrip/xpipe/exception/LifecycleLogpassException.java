package com.ctrip.xpipe.exception;


/**
 * abort lifecycle procedure
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
public class LifecycleLogpassException extends XpipeException{

	private static final long serialVersionUID = 1L;

	public LifecycleLogpassException(String message) {
		super(message);
	}

}
