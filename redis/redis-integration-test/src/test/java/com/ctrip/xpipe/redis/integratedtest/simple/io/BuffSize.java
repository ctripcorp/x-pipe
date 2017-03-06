package com.ctrip.xpipe.redis.integratedtest.simple.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @author wenchao.meng
 *
 * Feb 28, 2017
 */
public class BuffSize{
	
	private static String host = System.getProperty("host", "127.0.0.1");
	
	private static int port = Integer.parseInt(System.getProperty("port", "6379"));
	
	private static int sendBuffSize = Integer.parseInt(System.getProperty("sendBuffSize", "0"));
	
	private static int receiveBuffSize = Integer.parseInt(System.getProperty("receiveBuffSize", "0"));
	
	@SuppressWarnings("resource")
	public static void main(String []argc) throws IOException{

		Socket s = new Socket();
		s.connect(new InetSocketAddress(host, port));
		
		if(sendBuffSize > 0){
			System.out.println("set sendBufferSize:" + sendBuffSize);
			s.setSendBufferSize(sendBuffSize);
		}
		
		if(receiveBuffSize > 0){
			System.out.println("set receiveBufferSize:" + receiveBuffSize);
			s.setSendBufferSize(receiveBuffSize);
		}
		
		System.out.println("sendBuff:" + s.getSendBufferSize());
		System.out.println("receiveBuff:" + s.getReceiveBufferSize());
		
	}
	
}
