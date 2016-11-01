package com.ctrip.xpipe.utils;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;


/**
 * @author wenchao.meng
 *
 * Nov 1, 2016
 */
public class StringUtilTest extends AbstractTest{
	
	@Test
	public void test(){
		
		Assert.assertEquals("a,b,c", StringUtil.join(",", null, "a", "b", "c"));;
		
		Assert.assertEquals("a,b,c", StringUtil.join(",", "a", null, "b", "c"));;

		Assert.assertEquals("a,b,c", StringUtil.join(",", "a", null, "b", "c", null));;

	}

}
