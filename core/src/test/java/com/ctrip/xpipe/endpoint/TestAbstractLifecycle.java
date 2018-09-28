package com.ctrip.xpipe.endpoint;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Jun 6, 2016
 */
public class TestAbstractLifecycle extends AbstractTest{
	
	private TestLifecycle lifecycle = new TestLifecycle();
	
	@Test(expected = IllegalStateException.class)
	public void testInit() throws Exception{
		
		lifecycle.initialize();
		Assert.assertTrue(lifecycle.getLifecycleState().isInitialized());
		
		lifecycle.initialize();
		
		
	}

	@Test(expected = IllegalStateException.class)
	public void testStart() throws Exception{
		
		lifecycle.initialize();
		lifecycle.start();
		Assert.assertTrue(lifecycle.getLifecycleState().isStarted());
		
		lifecycle.start();
	}


	@Test(expected = IllegalStateException.class)
	public void testStop() throws Exception{
		
		lifecycle.initialize();
		lifecycle.start();
		lifecycle.stop();
		Assert.assertTrue(lifecycle.getLifecycleState().isStopped());
		
		lifecycle.stop();
	}


	@Test(expected = IllegalStateException.class)
	public void testDispose() throws Exception{
		
		lifecycle.initialize();
		lifecycle.start();
		lifecycle.stop();
		lifecycle.dispose();
		Assert.assertTrue(lifecycle.getLifecycleState().isDisposed());
		
		lifecycle.dispose();
	}

	public static class TestLifecycle extends AbstractLifecycle{} 

}
