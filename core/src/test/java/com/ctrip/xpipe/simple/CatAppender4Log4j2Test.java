package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Test;

/**
 * @author shyin
 *
 *         Sep 22, 2016
 */
public class CatAppender4Log4j2Test extends AbstractTest {

	@Test
	public void catAppender4Log4j2Test() {
		logger.error("error!!!!!!", new Exception("Test Exception occured!!!"));
	}
}
