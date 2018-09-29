package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Jun 17, 2016
 */
public class CreatedComponentRedistryTest extends AbstractTest{
	
	
	@Test
	public void test() throws Exception{
		
		CreatedComponentRedistry registry = new CreatedComponentRedistry();
		String name = registry.add(new Object());
		logger.info("{}", name);
		
		Lifecycle lifecycle = new NoOpLifecycleObject();
		registry.add(lifecycle);
		
		registry.initialize();
		registry.start();
		
		Assert.assertTrue(lifecycle.getLifecycleState().isStarted());
		
		Lifecycle lifecycle2 = new NoOpLifecycleObject();
		registry.add(lifecycle2);
		
		Assert.assertTrue(lifecycle2.getLifecycleState().isStarted());
		
		registry.remove(lifecycle2);
		
		Assert.assertTrue(lifecycle2.getLifecycleState().isDisposed());

		
		Assert.assertEquals(2, registry.allComponents().size());
		
	}

}
