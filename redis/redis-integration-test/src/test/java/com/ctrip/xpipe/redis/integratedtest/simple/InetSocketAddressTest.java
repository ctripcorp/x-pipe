package com.ctrip.xpipe.redis.integratedtest.simple;

import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *
 * Jan 10, 2017
 */
public class InetSocketAddressTest {
	
	

	public static void main(String[] args) {

		String ip = System.getProperty("ip", "10.2.58.242");
		int port = Integer.parseInt(System.getProperty("port", "8080"));
		boolean hostname = Boolean.parseBoolean(System.getProperty("hostname", "true"));

		System.out.println(String.format("%s:%d", ip, port));

		InetSocketAddress address = new InetSocketAddress(ip, port);
		

		long before = System.currentTimeMillis();
		if(hostname){
			System.out.println("hostname:" + address.getHostName());
		}else{
			System.out.println("hoststring:" + address.getHostString());
		}
		long after = System.currentTimeMillis();
		
		System.out.println(after - before);

	}

}
