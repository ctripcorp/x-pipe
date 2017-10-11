package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Sep 1, 2016
 */
public class MetaJacksonTest extends AbstractRedisTest{
	
	
	@Test
	public void testJackson(){
		
		DcMeta dcMeta = createDcMeta();
		String result = Codec.DEFAULT.encode(dcMeta);
		logger.info("{}", result);
		DcMeta decode = Codec.DEFAULT.decode(result, DcMeta.class);
		logger.info("{}", decode);
		
	}

	private DcMeta createDcMeta() {
		DcMeta dcMeta = new DcMeta();
		dcMeta.setId(getTestName());
		return dcMeta;
	}

}
