package com.ctrip.xpipe.redis.integratedtest.multidc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.exec.ExecuteException;
import org.junit.Test;

import com.ctrip.xpipe.redis.keeper.exception.RedisSlavePromotionException;
import com.ctrip.xpipe.redis.meta.server.exception.RedisMetaServerException;

/**
 * @author wenchao.meng
 *
 * Jun 21, 2016
 */
public class RedisFailOverTest extends AbstractMultiDcTest{
	
	@Test
	public void testFailOver() throws RedisSlavePromotionException, ExecuteException, IOException, RedisMetaServerException, InterruptedException, ExecutionException{
		
		failOverTestTemplate();

	}
}
