package com.ctrip.xpipe.redis.meta.server.rest.impl;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.rest.DefaultRestProcessTemplate;
import com.ctrip.xpipe.rest.ProcessTemplate;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:34:49 PM
 */
public class AbstractController {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ProcessTemplate processTemplate = new DefaultRestProcessTemplate();

}
