package com.ctrip.xpipe.simple;

import org.junit.Test;

import javax.annotation.processing.Processor;
import java.util.ServiceLoader;

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
