package com.ctrip.xpipe.lifecycle;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;


/**
 * @author wenchao.meng
 *
 * Dec 15, 2016
 */
public class DefaultRegistryTest extends AbstractTest{
	
	
	private Lifecycle lifecycle1; 
	
	private DefaultRegistry defaultRegistry;
	
	private ConfigurableApplicationContext applicationContext;
	
	@Before
	public void beforeDefaultRegistryTest() throws Exception{

		CreatedComponentRedistry created = new CreatedComponentRedistry();
		
		lifecycle1 = new NoOpLifecycleObject();
		created.add(lifecycle1);
		
		applicationContext = new AnnotationConfigApplicationContext(TestFactory.class);
		SpringComponentRegistry springRegistry = new SpringComponentRegistry(applicationContext);
		
		defaultRegistry = new DefaultRegistry(created, springRegistry);
		defaultRegistry.initialize();
		defaultRegistry.start();
	}
	
	@Test
	public void testNormal() throws Exception{
		
		defaultRegistry.stop();
		defaultRegistry.dispose();
		defaultRegistry.destroy();
		Assert.assertTrue(lifecycle1.getLifecycleState().isDisposed());
		Assert.assertFalse(applicationContext.isActive());
	}
	
	@Test
	public void testDestroy() throws Exception{

		defaultRegistry.destroy();
		Assert.assertTrue(lifecycle1.getLifecycleState().isDisposed());
		Assert.assertFalse(applicationContext.isActive());
	}

	
	@Configuration
	public static class TestFactory{
		
		@Bean
		public Lifecycle create1(){
			return new NoOpLifecycleObject();
		}

	}
	

}
