package com.ctrip.xpipe.exception;

/**
 * @author wenchao.meng
 *
 * Aug 11, 2016
 */
public interface ErrorMessageAware {
	
	ErrorMessage<?> getErrorMessage();

}
