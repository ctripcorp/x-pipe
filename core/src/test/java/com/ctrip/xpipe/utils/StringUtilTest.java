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
	public void testToString(){

		Assert.assertEquals(String.valueOf(Integer.MAX_VALUE), StringUtil.toString(Integer.MAX_VALUE));

		int []intArr = new int[]{1, 2, 3};
		Assert.assertEquals("[1, 2, 3]", StringUtil.toString(intArr));

		Object [] arrInArr = new Object[]{"a", intArr, "b"};
		Assert.assertEquals("[a, [1, 2, 3], b]", StringUtil.toString(arrInArr));

	}
	
	@Test
	public void test(){
		
		Assert.assertEquals("a,null,b,c", StringUtil.join(",", "a", null, "b", "c"));
		Assert.assertEquals("a,null,b,c,null", StringUtil.join(",", "a", null, "b", "c", null));

	}
	
	@Test
	public void testTrimEquals(){
		
		Assert.assertTrue(StringUtil.trimEquals(" abc ", " abc  \t"));
		Assert.assertFalse(StringUtil.trimEquals(null, " abc  \t"));
		Assert.assertFalse(StringUtil.trimEquals("abc ", null));
		
	}

}
