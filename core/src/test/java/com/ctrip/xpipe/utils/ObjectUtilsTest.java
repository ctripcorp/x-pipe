package com.ctrip.xpipe.utils;

import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;


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
