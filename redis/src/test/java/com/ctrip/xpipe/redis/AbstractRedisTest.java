package com.ctrip.xpipe.redis;


import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.Assert;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;

import io.netty.buffer.ByteBufAllocator;
import redis.clients.jedis.Jedis;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:54:09
 */
public abstract class AbstractRedisTest extends AbstractTest{

	protected ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

	protected static final int runidLength = 40;
	

	protected String readLine(InputStream ins) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		int last = 0;
		while(true){
			
			int data = ins.read();
			if(data == -1){
				return null;
			}
			sb.append((char)data);
			if(data == '\n' && last == '\r'){
				break;
			}
			last = data;
		}
		
		return sb.toString();
	}


	protected Jedis createJedis(RedisMeta redisMeta) {
		
		Jedis jedis = new Jedis(redisMeta.getIp(), redisMeta.getPort()); 
		logger.info("[createJedis]{}", jedis);
		return jedis;
	}

	protected void assertRedisEquals(RedisMeta redisMaster, List<RedisMeta> redisSlaves) {
		
		Map<String, String> values = new HashMap<>(); 
		Jedis jedis = createJedis(redisMaster);
		Set<String> keys = jedis.keys("*");
		for(String key : keys){
			values.put(key, jedis.get(key));
		}

		for(RedisMeta redisSlave : redisSlaves){
			
			Jedis slave = createJedis(redisSlave);
			Assert.assertEquals(values.size(), slave.keys("*").size());
			
			for(Entry<String, String> entry : values.entrySet()){
				
				String realValue = slave.get(entry.getKey());
				Assert.assertEquals(entry.getValue(), realValue);
			}
		}
		
		
	}

	protected void sendRandomMessage(RedisMeta redisMeta, int count) {
		
		Jedis jedis = createJedis(redisMeta);
		
		logger.info("[sendRandomMessage][begin]{}", jedis);
		for(int i=0; i < count; i++){
			jedis.set(String.valueOf(i), randomString());
			jedis.incr("incr");
		}
		logger.info("[sendRandomMessage][end  ]{}", jedis);
	}


}
