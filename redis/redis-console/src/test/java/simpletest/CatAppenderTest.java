package simpletest;

import org.junit.Test;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;

/**
 * @author shyin
 *
 * Aug 11, 2016
 */

public class CatAppenderTest extends AbstractConsoleTest{
	
	@Test
	public void catAppenderTest() throws InterruptedException {
		logger.error("CatAppender error message", new Exception("simple exception"));
	}

}
