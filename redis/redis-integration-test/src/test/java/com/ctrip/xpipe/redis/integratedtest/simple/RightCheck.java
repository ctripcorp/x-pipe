package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.IpUtils;
import redis.clients.jedis.Jedis;

import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * foramt
 * @author wenchao.meng
 *
 * Nov 21, 2016
 */
public class RightCheck extends AbstractStartStoppable{
	
	private String configFile = System.getProperty("configFile", "check_servers.properties");
	private int expireTime = 10;
	
	
	public static void main(String []argc) throws Exception{
		
		new RightCheck().start();
		
	}

	@Override
	protected void doStart() throws Exception {

		InputStream ins = FileUtils.getFileInputStream(configFile);
		Properties properties = new Properties();
		properties.load(ins);
		
		for(Object key : properties.keySet()){
			test((String)key, (String)properties.get(key));
		}
	}

	private void test(String key, String address) throws InterruptedException {
		
		logger.info("=========[test]{},{}=========", key, address);
		String []sp = address.split("\\s*(;|,)\\s*");
		
		String testKey = "xpipe-test-key";
		String testValue = UUID.randomUUID().toString();
		
		Jedis master = getJedis(sp[0]);
		logger.info("[test][write to master]{}, {}, {}", sp[0], key, address);
		master.setex(testKey, expireTime, testValue);
		
		TimeUnit.MILLISECONDS.sleep(100);
		
		for(int i=1; i < sp.length; i++){
			
			logger.info("[test][test slave]{}", sp[i]);
			
			Jedis slave = getJedis(sp[i]);
			String realValue = slave.get(testKey);
			if(!testValue.equals(realValue)){
				logger.error("[not equal]{}, {}, {} but: {}", sp[i], testKey, testValue, realValue);
			}
		}
		
	}

	private Jedis getJedis(String addr) {
		
		Pair<String, Integer> pair = IpUtils.parseSingleAsPair(addr);
		return new Jedis(pair.getKey(), pair.getValue());
	}

	@Override
	protected void doStop() {
		
	}
}
