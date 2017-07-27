package com.ctrip.xpipe.utils;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * Aug 8, 2016
 */
public class FileUtilsTest extends AbstractTest{
	
	@Test
	public void test() throws FileNotFoundException{
		
		Assert.assertNotNull(FileUtils.getFileInputStream("fileutiltest.txt"));

		try{
			FileUtils.getFileInputStream("fileutiltest_not_exist.txt");
			Assert.fail();
		}catch(FileNotFoundException e){
			
		}
	}
	
	@Test
	public void testAbsolute() throws IOException{
		
		String testDir = getTestFileDir();
		logger.info("[testAbsolute]{}", testDir);
		
		String fileName = getTestName() + ".txt";
		File file = new File(testDir, fileName);
		String value = randomString();
		
		org.apache.commons.io.FileUtils.write(file, value);

		InputStream ins = FileUtils.getFileInputStream(new File(testDir).getAbsolutePath(), fileName);
		
		Assert.assertNotNull(ins);
	}
	
	@Test
	public void testShortPath(){
		
		Assert.assertEquals("#a#b#c", FileUtils.shortPath("#a#b#c", 2));
		Assert.assertEquals("/d/e", FileUtils.shortPath("/a/b/c/d/e", 2));
		Assert.assertEquals("/a/b/c/d/e", FileUtils.shortPath("/a/b/c/d/e", 5));
		Assert.assertEquals("/a/b/c/d/e", FileUtils.shortPath("/a/b/c/d/e", 6));
	}

}
