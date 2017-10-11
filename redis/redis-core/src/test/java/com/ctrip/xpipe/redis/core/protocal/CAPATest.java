package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Feb 17, 2017
 */
public class CAPATest extends AbstractRedisTest{
	
	@Test
	public void test(){
		
		Assert.assertEquals(CAPA.EOF, CAPA.of("eof"));
		Assert.assertEquals(CAPA.EOF, CAPA.of("EoF"));
		Assert.assertEquals(CAPA.EOF, CAPA.of("EOF"));

		Assert.assertEquals(CAPA.PSYNC2, CAPA.of("psync2"));
		Assert.assertEquals(CAPA.PSYNC2, CAPA.of("PsYNC2"));
		Assert.assertEquals(CAPA.PSYNC2, CAPA.of("PSYNC2"));

	}

}
