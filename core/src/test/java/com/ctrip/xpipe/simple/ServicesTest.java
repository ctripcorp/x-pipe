package com.ctrip.xpipe.simple;

import java.util.ServiceLoader;

import javax.annotation.processing.Processor;

import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Dec 6, 2016
 */
public class ServicesTest {
	
	@Test
	public void test(){
		
		ServiceLoader<Processor> services = ServiceLoader.load(Processor.class);
		for(Processor service : services){
			System.out.println(service);
		}
		
		
	}

}
