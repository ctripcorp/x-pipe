package com.ctrip.xpipe.redis.core.meta;

import org.junit.Test;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * @author wenchao.meng
 *
 * Oct 17, 2016
 */
public class DcMetaTest extends AbstractRedisTest{
	
	
	@Test
	public void test(){
		
		DcMeta dcMeta = getDcMeta("jq");
		String dcMetaStr = Codec.DEFAULT.encode(dcMeta);
		DcMeta dcMetaDe = Codec.DEFAULT.decode(dcMetaStr, DcMeta.class);
		
		logger.info("[test]{}", dcMeta.getClusters().get("cluster1").parent());
		logger.info("[test]{}", dcMetaDe.getClusters().get("cluster1").parent());
	}
	
	

	
	@Override
	protected String getXpipeMetaConfigFile() {
		return "dc-meta-test.xml";
	}
}
