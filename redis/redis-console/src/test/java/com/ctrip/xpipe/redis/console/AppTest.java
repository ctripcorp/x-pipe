package com.ctrip.xpipe.redis.console;

import java.io.IOException;

import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author lepdou 2016-11-09
 */
@SpringBootApplication
public class AppTest extends AbstractConsoleTest{
	
	@Test
	public void startConsole() throws IOException{
		
		System.setProperty("FXXPIPE_HOME", "/opt/data");
		
		SpringApplication.run(AppTest.class);
		
		waitForAnyKeyToExit();
	}

}
