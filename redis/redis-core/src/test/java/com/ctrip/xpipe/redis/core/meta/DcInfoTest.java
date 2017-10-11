package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Nov 3, 2016
 */
public class DcInfoTest extends AbstractRedisTest{
	
	@Test
	public void testMap(){
		
		Map<String, DcInfo> dcInfos = new HashMap<>();
		
		dcInfos.put("jq", new DcInfo("address1"));
		dcInfos.put("oy", new DcInfo("address2"));

		String encode = JsonCodec.INSTANCE.encode(dcInfos);
		
		logger.info("[testMap]{}", encode);
		
		Map<String, DcInfo> decode = JsonCodec.INSTANCE.decode(encode, new GenericTypeReference<Map<String, DcInfo>>() {
		});
		
		Assert.assertEquals(dcInfos, decode);
	}

}
