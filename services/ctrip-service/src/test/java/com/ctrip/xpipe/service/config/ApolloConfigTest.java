package com.ctrip.xpipe.service.config;

import com.ctrip.xpipe.service.AbstractServiceTest;
import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

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
