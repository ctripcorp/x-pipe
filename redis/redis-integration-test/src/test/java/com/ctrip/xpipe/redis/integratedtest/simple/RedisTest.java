package com.ctrip.xpipe.redis.integratedtest.simple;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Jan 20, 2017
 */
public class RedisTest extends AbstractIntegratedTest{
	
	@Test
	public void test() throws IOException{

		Socket s = new Socket();
		s.connect(new InetSocketAddress("localhost", 6379));
		
		for(String command : createCommands()){
			s.getOutputStream().write(command.getBytes());
		}
		
		s.close();
	}

	@Test
	public void testIncr() throws IOException {

		Jedis jedis = createJedis("localhost", 6379);

		scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {
			@Override
			protected void doRun() throws Exception {
				jedis.incr("incr");

			}
		}, 0, 100, TimeUnit.MILLISECONDS);


		waitForAnyKeyToExit();
	}
	
	private String[] createCommands() {

		int valueLen = 20;
		
		String setcommand = "set b ";
		setcommand += randomString(valueLen - setcommand.length());
		
		return new String[] { 
				"*3\r\n"
				+ "$3\r\nset\r\n"
				+ "$1\r\na\r\n"
				+ "$" + valueLen + "\r\n"
				, setcommand + "\r\n1\r\n" };
	}


	@Override
	protected List<RedisMeta> getRedisSlaves() {
		return null;
	}
}
