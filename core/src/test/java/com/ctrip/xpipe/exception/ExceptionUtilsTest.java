package com.ctrip.xpipe.exception;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author wenchao.meng
 *
 * Oct 31, 2016
 */
public class ExceptionUtilsTest extends AbstractTest{

	@Test
	public void testLog(){
		
	}

	@Test
	public void testGetTypeAndMessage() {
		assertEquals("IOException: Input/output error", ExceptionUtils.getTypeAndMessage(new IOException("Input/output error")));
		assertEquals("IllegalStateException: null", ExceptionUtils.getTypeAndMessage(new IllegalStateException()));
		assertNull(ExceptionUtils.getTypeAndMessage(null));
	}
}
