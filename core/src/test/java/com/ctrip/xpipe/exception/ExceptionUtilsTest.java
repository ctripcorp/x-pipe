package com.ctrip.xpipe.exception;

import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;

/**
 * @author wenchao.meng
 *
 * Oct 31, 2016
 */
public class ExceptionUtilsTest extends AbstractTest{

	@Test
	public void testLog(){
		
		ExceptionUtils.logException(logger, new HttpServerErrorException(HttpStatus.BAD_GATEWAY, "text", randomString().getBytes(), Codec.defaultCharset));
	}
}
