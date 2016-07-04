package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.exception.XpipeException;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public class BorrowObjectException extends XpipeException{

	private static final long serialVersionUID = 1L;

	public BorrowObjectException(String message) {
		super(message);
	}

	public BorrowObjectException(String message, Throwable th) {
		super(message, th);
	}

}
