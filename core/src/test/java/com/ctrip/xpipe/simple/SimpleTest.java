package com.ctrip.xpipe.simple;


import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.dianping.cat.configuration.client.entity.ClientConfig;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class SimpleTest extends AbstractTest{
	
	@Test
	public <V> void testCommand(){
		ClientConfig clientConfig = new ClientConfig();
		System.out.println(clientConfig);
	}
	
	@Test
	public void testThread(){
		
		Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException e) {
				}
			}
		});
		
		logger.info("[testThread]{}", thread.isAlive());
		
		thread.start();
		
		sleep(1000);
		logger.info("[testThread]{}", thread.isAlive());

		sleep(5000);
		logger.info("[testThread]{}", thread.isAlive());
		
		
	}
}
