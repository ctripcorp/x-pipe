package com.ctrip.xpipe.retry;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.retry.RetryWait;

/**
 * @author wenchao.meng
 *
 * Jul 9, 2016
 */
public abstract class AbstractRetryWait implements RetryWait{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

}
