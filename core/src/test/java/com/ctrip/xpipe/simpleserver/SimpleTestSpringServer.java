package com.ctrip.xpipe.simpleserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * @author shyin
 *
 * Sep 20, 2016
 */
@EnableAutoConfiguration
@Import(com.ctrip.xpipe.simpleserver.SimpleTestSpringConfiguration.class)
public class SimpleTestSpringServer{
	
	public static void main(String[] args) throws Exception {
		SpringApplication.run(SimpleTestSpringServer.class, "");
	}

}
