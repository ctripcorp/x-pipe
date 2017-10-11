package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


/**
 * @author wenchao.meng
 *
 * Jul 17, 2016
 */
public class RedisMetaTest extends AbstractRedisTest{
	
	@Test
	public void testJson() throws JsonParseException, JsonMappingException, IOException{
		
		RedisMeta redisMeta = new RedisMeta();
		
		redisMeta.setId("id").setIp("ip").setMaster("mastr").setOffset(1111L);
		ObjectMapper mapper = new ObjectMapper();
		
		String json = mapper.writeValueAsString(redisMeta);

		System.out.println(json);
		
		RedisMeta meta = mapper.readValue(json, RedisMeta.class);
		
		Assert.assertEquals(redisMeta, meta);
		
	}

}
