package com.ctrip.xpipe.redis.meta.server.simpletest;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author wenchao.meng
 *
 * Aug 10, 2016
 */
public class LogTest {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
		
	@Test
	public void test(){
		
		logger.info("[info]");
		
		logger.error("[error]", new Exception());
	}

}
