package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class ReturnObjectException extends XpipeException{

	private static final long serialVersionUID = 1L;

	public ReturnObjectException(String message) {
		super(message);
	}

	public ReturnObjectException(String message, Throwable th) {
		super(message, th);
	}

}
