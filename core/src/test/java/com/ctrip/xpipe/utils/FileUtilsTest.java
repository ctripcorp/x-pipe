package com.ctrip.xpipe.utils;

import java.io.FileNotFoundException;

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
		
		Assert.assertNotNull(FileUtils.getFileInputStream("fileutiltest.txt"));;

		try{
			FileUtils.getFileInputStream("fileutiltest_not_exist.txt");
			Assert.fail();
		}catch(FileNotFoundException e){
			
		}
}

}
