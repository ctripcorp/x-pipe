package com.ctrip.xpipe.simple;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.channels.FileChannel;

import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.simpleserver.Server;

/**
 * @author wenchao.meng
 *
 * Dec 6, 2016
 */
public class StreamTest extends AbstractTest{
	
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
	

}
