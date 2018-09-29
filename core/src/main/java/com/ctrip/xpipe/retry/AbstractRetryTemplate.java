package com.ctrip.xpipe.retry;


import com.ctrip.xpipe.api.retry.RetryTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public abstract class AbstractRetryTemplate<V> implements RetryTemplate<V>{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

}
