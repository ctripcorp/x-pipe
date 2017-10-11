package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public class PrimaryDcCheckMessageTest extends AbstractRedisTest{
	
	
	@Test
	public void testEncodeDecode(){
		
		PrimaryDcCheckMessage message = new PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT.SUCCESS, "hello");
		String encode = Codec.DEFAULT.encode(message);
		
		logger.info("{}", encode);
		
		PrimaryDcCheckMessage decode = Codec.DEFAULT.decode(encode, PrimaryDcCheckMessage.class);
		
		logger.info("{}", decode);
		
	}

}
