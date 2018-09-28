package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import org.junit.Assert;
import org.junit.Test;



/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class DefaultLifecycleStateTest extends AbstractTest{
	
	private Lifecycle lifecycle = new NoOpLifecycleObject();
	
	@Test
	public void testGetPhase() throws Exception{

		Assert.assertFalse(lifecycle.getLifecycleState().isInitialized());
		Assert.assertFalse(lifecycle.getLifecycleState().isStarted());
		Assert.assertTrue(lifecycle.getLifecycleState().isStopped());
		Assert.assertTrue(lifecycle.getLifecycleState().isDisposed());

		lifecycle.initialize();
		
		Assert.assertTrue(lifecycle.getLifecycleState().isInitialized());
		Assert.assertFalse(lifecycle.getLifecycleState().isStarted());
		Assert.assertTrue(lifecycle.getLifecycleState().isStopped());
		Assert.assertFalse(lifecycle.getLifecycleState().isDisposed());
		
		lifecycle.start();

		Assert.assertTrue(lifecycle.getLifecycleState().isInitialized());
		Assert.assertTrue(lifecycle.getLifecycleState().isStarted());
		Assert.assertFalse(lifecycle.getLifecycleState().isStopped());
		Assert.assertFalse(lifecycle.getLifecycleState().isDisposed());

		lifecycle.stop();

		Assert.assertTrue(lifecycle.getLifecycleState().isInitialized());
		Assert.assertFalse(lifecycle.getLifecycleState().isStarted());
		Assert.assertTrue(lifecycle.getLifecycleState().isStopped());
		Assert.assertFalse(lifecycle.getLifecycleState().isDisposed());

		lifecycle.dispose();

		Assert.assertFalse(lifecycle.getLifecycleState().isInitialized());
		Assert.assertFalse(lifecycle.getLifecycleState().isStarted());
		Assert.assertTrue(lifecycle.getLifecycleState().isStopped());
		Assert.assertTrue(lifecycle.getLifecycleState().isDisposed());
}
	
	

}
