package com.ctrip.xpipe.simple;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.dianping.cat.Cat;

/**
 * @author wenchao.meng
 *
 * Dec 6, 2016
 */
public class CatTest extends AbstractTest{
	
	@Test
	public void testLog(){
		
		Cat.getProducer().logError(new Exception());
	}

}
