package com.ctrip.xpipe.redis.meta.server.spring.application;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.MetaServerApplication;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Test;
import org.springframework.boot.SpringApplication;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Aug 30, 2016
 */
public class ApplicationTest extends AbstractMetaServerTest{
	
	@Test
	public void testRun() throws IOException{
		
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_PRODUCTION);
		System.setProperty("TOTAL_SLOTS", String.valueOf(16));

		System.setProperty(DefaultDcMetaCache.MEMORY_META_SERVER_DAO_KEY, "metaserver--jq.xml");
		startZk(2181);
		new SpringApplication(MetaServerApplication.class).run();
		
		waitForAnyKeyToExit();
	}
	

}
