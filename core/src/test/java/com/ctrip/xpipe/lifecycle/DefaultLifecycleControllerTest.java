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
public class DefaultLifecycleControllerTest extends AbstractTest{
		
	@Test
	public void testCanInitialize() throws Exception{
		
		Lifecycle lifecycle = new NoOpLifecycleObject();
		lifecycle.initialize();
		
		
		try{
			lifecycle.initialize();
			Assert.fail();
		}catch(Exception e){
		}

		lifecycle.start();

		try{
			lifecycle.initialize();
			Assert.fail();
		}catch(Exception e){
		}
		
		lifecycle.stop();
		try{
			lifecycle.initialize();
			Assert.fail();
		}catch(Exception e){
		}

		lifecycle.dispose();
		lifecycle.initialize();

	}
	
	@Test
	public void testCanStart() throws Exception{
		
		Lifecycle lifecycle = new NoOpLifecycleObject();

		try{
			lifecycle.start();
			Assert.fail();
		}catch(Exception e){
		}

		lifecycle.initialize();
		
		lifecycle.start();
		
		lifecycle.stop();
		
		lifecycle.start();
		
		lifecycle.stop();
		lifecycle.dispose();
		
		try{
			lifecycle.start();
			Assert.fail();
		}catch(Exception e){
		}
		
		
	}

	@Test
	public void testCanStop() throws Exception{

		Lifecycle lifecycle = new NoOpLifecycleObject();

		try{
			lifecycle.stop();
			Assert.fail();
		}catch(Exception e){
		}
		
		lifecycle.initialize();
		try{
			lifecycle.stop();
			Assert.fail();
		}catch(Exception e){
		}

		lifecycle.start();
		
		lifecycle.stop();

		
		lifecycle.dispose();
		try{
			lifecycle.stop();
			Assert.fail();
		}catch(Exception e){
		}


		
		
	}

	@Test
	public void testCanDispose() throws Exception{
		
		Lifecycle lifecycle = new NoOpLifecycleObject();

		try{
			lifecycle.dispose();
			Assert.fail();
		}catch(Exception e){
		}

		lifecycle.initialize();
		
		lifecycle.dispose();
		
		lifecycle.initialize();
		lifecycle.start();
		

		try{
			lifecycle.dispose();
			Assert.fail();
		}catch(Exception e){
		}

		lifecycle.stop();

		lifecycle.dispose();

		try{
			lifecycle.dispose();
			Assert.fail();
		}catch(Exception e){
		}
	}

}
