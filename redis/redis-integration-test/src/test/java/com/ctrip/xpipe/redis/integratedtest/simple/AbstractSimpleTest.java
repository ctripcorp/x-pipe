package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.AbstractTest;
import org.junit.BeforeClass;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
public abstract class AbstractSimpleTest extends AbstractTest{
	
	@BeforeClass
	public static void beforeClass(){
		System.out.println("beforeClass");
	}

}
