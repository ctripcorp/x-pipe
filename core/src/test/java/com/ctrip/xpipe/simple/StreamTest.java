package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;

/**
 * @author wenchao.meng
 *
 * Dec 6, 2016
 */
public class StreamTest extends AbstractTest{
	
	@SuppressWarnings("resource")
	@Test
	public void testClose() throws Exception{
		
		Server server = startEchoServer();
		
		Socket s = new Socket("localhost", server.getPort());
		
		OutputStream ous = s.getOutputStream();
		
		ous.write("123".getBytes());
		
		ous.close();
		ous.write("123".getBytes());
		
	}
	
	@Test
	public void testIoOf() throws IOException{
		
		RandomAccessFile randomAccessFile = new RandomAccessFile("/opt/logs/test.log", "r");
		
		logger.info("[testIoOf]{}", randomAccessFile);
		FileChannel channel = randomAccessFile.getChannel();
		
		randomAccessFile.close();
		randomAccessFile.close();
		
		channel.close();
	}
	
	@Test
	public void testSendBuff() throws Exception{
		
		Server server = startEchoServer();
		@SuppressWarnings("resource")
		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", server.getPort()));
		
		System.out.println(socket.getSendBufferSize());
		System.out.println(socket.getReceiveBufferSize());
	}
	
}
