package com.ctrip.xpipe.utils.log;

import java.io.IOException;
import java.nio.charset.Charset;

import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * Nov 1, 2016
 */
public class LogTest extends AbstractTest{
	
	@Test
	public void testLog() throws IOException{
		
		logger.error("[testLog]", new IOException("io exception"));
		logger.error("[testLog]", new HttpServerErrorException(HttpStatus.BAD_GATEWAY, "statusCode",
				"responseBodyExample".getBytes(), Charset.defaultCharset()));

		logger.error("[testLog]", new Exception("simple exception"));
		
	}

}
