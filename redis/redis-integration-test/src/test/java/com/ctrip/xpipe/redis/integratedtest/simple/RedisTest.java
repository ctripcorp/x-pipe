package com.ctrip.xpipe.redis.integratedtest.simple;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * Jan 20, 2017
 */
public class RedisTest extends AbstractTest{
	
	@Test
	public void test() throws IOException{

		Socket s = new Socket();
		s.connect(new InetSocketAddress("localhost", 6379));
		
		for(String command : createCommands()){
			s.getOutputStream().write(command.getBytes());
		}
		
		s.close();
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

}
