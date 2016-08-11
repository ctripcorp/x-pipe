package com.ctrip.xpipe.redis.meta.server.simpletest;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;


/**
 * @author wenchao.meng
 *
 * Aug 10, 2016
 */
public class LogTest extends AbstractMetaServerTest{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
		
	@Test
	public void test() throws IOException{
		
		logger.info("[info]");
		
		logger.error("[error]", new Exception());
		
		waitForAnyKeyToExit();
	}

}
