package com.ctrip.xpipe.redis.keeper.config;


import org.junit.Test;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class DefaultKeeperConfigTest extends AbstractRedisKeeperTest{
	
	@Test
	public void testKeeperConfig(){
		
		KeeperConfig keeperConfig = new DefaultKeeperConfig();
		logger.info("[keeperConfig]{}, {}", Codec.DEFAULT.encode(keeperConfig), keeperConfig);
		
		
	}

}
