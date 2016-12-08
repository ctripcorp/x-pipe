package com.ctrip.xpipe.redis.integratedtest.simple;

import org.junit.BeforeClass;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
public abstract class AbstractSimpleTest {
	
	@BeforeClass
	public static void beforeClass(){
		System.out.println("beforeClass");
	}

}
