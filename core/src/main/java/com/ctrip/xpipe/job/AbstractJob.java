package com.ctrip.xpipe.job;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.job.Job;
import com.ctrip.xpipe.api.job.JobFuture;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public abstract class AbstractJob<V> implements Job<V>{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public JobFuture<V> execute() {
		return doExecute();
	}

	protected abstract JobFuture<V> doExecute();

}
