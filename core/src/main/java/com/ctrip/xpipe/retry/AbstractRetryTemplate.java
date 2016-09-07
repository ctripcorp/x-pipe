package com.ctrip.xpipe.retry;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.retry.RetryTemplate;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public abstract class AbstractRetryTemplate<V> implements RetryTemplate<V>{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

}
