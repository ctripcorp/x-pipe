package com.ctrip.xpipe.api.job;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public interface Job<V> {
	
	
	JobFuture<V> execute();
	
}
