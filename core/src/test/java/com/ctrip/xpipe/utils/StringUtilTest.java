package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Nov 1, 2016
 */
public class StringUtilTest extends AbstractTest{

	@Test
	public void testMakeSimpleName(){

		Assert.assertEquals("[a.b]", StringUtil.makeSimpleName("a","b"));
		Assert.assertEquals("[a.b]", StringUtil.makeSimpleName("a","a.b"));
		Assert.assertEquals("[a.b]", StringUtil.makeSimpleName(null,"a.b"));
		Assert.assertEquals("[a]", StringUtil.makeSimpleName("a",null));
		Assert.assertEquals("[null]", StringUtil.makeSimpleName(null,null));
	}

	@Test
	public void testSubHead(){

		Assert.assertEquals("12345", StringUtil.subHead("12345", 5));
		Assert.assertEquals("12345", StringUtil.subHead("123456", 5));

	}

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

	@Test
	public void testCompareVersion() {
		String version = "2.8.19";
		String targetVersion = "2.8.22";
		Assert.assertEquals(-1, StringUtil.compareVersion(version, targetVersion));
		version = "2.8.19.1";
		Assert.assertEquals(-1, StringUtil.compareVersion(version, targetVersion));
		version = "2.8";
		Assert.assertEquals(-1, StringUtil.compareVersion(version, targetVersion));
		version = "2.8.22.1";
		Assert.assertEquals(1, StringUtil.compareVersion(version, targetVersion));
	}

	@Test
	public void testCompareVersionSize() {
		String targetVersion = "1.3";
		String version = null;
		try {
			StringUtil.compareVersionSize(version, targetVersion);
		} catch (Exception e) {
			Assert.assertEquals("version cannot be null", e.getMessage());
		}
		version = "1.a";
		try {
			StringUtil.compareVersionSize(version, targetVersion);
		} catch (Exception e) {
			Assert.assertEquals("version contains non-numeric characters", e.getMessage());
		}
		version = "1.3";
		Assert.assertEquals(0, StringUtil.compareVersionSize(version, targetVersion));
		version = "1.2";
		Assert.assertEquals(-1, StringUtil.compareVersionSize(version, targetVersion));
		version = "2.0";
		Assert.assertEquals(1, StringUtil.compareVersionSize(version, targetVersion));
		version = "2.a";
		Assert.assertEquals(1, StringUtil.compareVersionSize(version, targetVersion));
	}

}
