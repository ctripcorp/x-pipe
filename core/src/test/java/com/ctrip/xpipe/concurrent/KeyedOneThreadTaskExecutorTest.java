package com.ctrip.xpipe.concurrent;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.command.TestCommand;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public class KeyedOneThreadTaskExecutorTest extends AbstractTest{
	
	private int sleepInterval = 100;
	private KeyedOneThreadTaskExecutor<String> keyed;
	
	@Before
	public void beforeKeyedOneThreadTaskExecutorTest(){
		 keyed = new KeyedOneThreadTaskExecutor<>(executors);
	}


	@Test
	public void testSameKey(){
		
		TestCommand command1 =  new TestCommand("success", sleepInterval);
		TestCommand command2 =  new TestCommand("success", sleepInterval);
		
		keyed.execute("key1", command1);
		keyed.execute("key1", command2);
		
		sleep(sleepInterval/2);
		
		Assert.assertTrue(command1.isBeginExecute());
		Assert.assertFalse(command2.isBeginExecute());
		
	}

	@Test
	public void testDifferentKey(){
		
		TestCommand command1 =  new TestCommand("success", sleepInterval);
		TestCommand command2 =  new TestCommand("success", sleepInterval);
		
		keyed.execute("key1", command1);
		keyed.execute("key2", command2);
		
		sleep(sleepInterval/2);
		
		Assert.assertTrue(command1.isBeginExecute());
		Assert.assertTrue(command2.isBeginExecute());
		
	}

	@After
	public void afterKeyedOneThreadTaskExecutorTest() throws Exception{
		 keyed.destroy();
	}
}
