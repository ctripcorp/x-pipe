package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Mar 21, 2017
 */
public class UrlUtilsTest extends AbstractTest{
	
	@Test
	public void testFormat(){
		
		Assert.assertEquals("http://localhost/a/b/c", UrlUtils.format("http://localhost/a/b/c"));
		Assert.assertEquals("localhost/a/b/c", UrlUtils.format("localhost/a/b/c"));
		
		Assert.assertEquals("http://localhost/a/b/c", UrlUtils.format("http://localhost////a/b//c"));
		Assert.assertEquals("localhost/a/b/c", UrlUtils.format("localhost///a/b//c"));
	}

}
