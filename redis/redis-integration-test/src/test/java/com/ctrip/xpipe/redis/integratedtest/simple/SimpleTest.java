package com.ctrip.xpipe.redis.integratedtest.simple;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Aug 17, 2016
 */
public class SimpleTest extends AbstractSimpleTest{
	

	@Test
	public void test(){
		System.out.println("nihaoma");
	}
	
	@Test
	public void testAlloc() throws InterruptedException{
		
		while(true){
			
			TimeUnit.MILLISECONDS.sleep(1);
			@SuppressWarnings("unused")
			byte [] data = new byte[ 1 << 10 ];
		}
		
	}
	

}
