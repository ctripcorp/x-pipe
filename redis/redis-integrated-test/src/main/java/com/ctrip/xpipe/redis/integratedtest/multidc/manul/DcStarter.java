package com.ctrip.xpipe.redis.integratedtest.multidc.manul;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;
import com.ctrip.xpipe.redis.integratedtest.multidc.AbstractMultiDcTest;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class DcStarter extends AbstractMultiDcTest{
	
	
	@Test
	public void startDc(){
		
	}


	@After
	public void afterDcStarter() throws IOException{
		
		waitForAnyKeyToExit();
	}
	
}
