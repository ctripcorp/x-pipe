package com.ctrip.xpipe.codec;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * Oct 25, 2016
 */
public class JsonCodecTest extends AbstractTest{
	
	@Test
	public void test(){
		
		JsonCodec jsonCodec = new JsonCodec(true, true);
		
		System.out.println(jsonCodec.encode("123\n345"));
	}
	
}
