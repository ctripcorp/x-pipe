package com.ctrip.xpipe.api.retry;

import java.util.concurrent.Callable;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public interface RetryTemplate {
	
	boolean execute(Callable<RetryType> action);

}
