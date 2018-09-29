package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;


/**
 * @author wenchao.meng
 *
 * Aug 4, 2016
 */
public class ObjectUtilsTest extends AbstractTest{
	
	@Test
	public void testGetMethod(){
		
		Method method = ObjectUtils.getMethod("testGetMethod", getClass());
		Assert.assertNotNull(method);
		
	}

}
