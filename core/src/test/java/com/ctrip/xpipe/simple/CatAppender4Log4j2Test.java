package com.ctrip.xpipe.simple;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shyin
 *
 * Sep 22, 2016
 */
public class CatAppender4Log4j2Test {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	@Test
	public void catAppender4Log4j2Test() {
		logger.error("error!!!!!!", new Exception("Test Exception occured!!!"));
	}
}
