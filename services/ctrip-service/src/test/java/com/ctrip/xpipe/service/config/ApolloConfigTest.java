package com.ctrip.xpipe.service.config;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractServiceTest;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public class ApolloConfigTest extends AbstractServiceTest{
	
	
	@Test
	public void testApollo(){
		
		Config config = Config.DEFAULT;
		Assert.assertTrue(config instanceof ApolloConfig);


		
		String result = config.get("test");
		logger.info("test value:{}", result);
		config.addConfigChangeListener(new ConfigChangeListener() {
			
			@Override
			public void onChange(String key, String oldValue, String newValue) {
				logger.info("{}:{}->{}", key, oldValue, newValue);
			}
		});
		
	}

	@After
	public void afterApolloConfigTest() throws IOException{
		waitForAnyKeyToExit();
	}
	

}
