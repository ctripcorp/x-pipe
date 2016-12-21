package com.ctrip.xpipe.redis.console.health.rawtcp;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * @author marsqing
 *
 *         Dec 9, 2016 4:02:23 PM
 */
public class Client {
	public static void main(String[] args) throws Exception {
		@SuppressWarnings("resource")
		Socket s = new Socket("127.0.0.1", 7777);
		s.setTcpNoDelay(true);

		OutputStream out = s.getOutputStream();
		InputStream in = s.getInputStream();
		byte[] buf = new byte[8];
		while (true) {
			out.write(longToBytes(System.nanoTime()));
			in.read(buf);
			System.out.println((System.nanoTime() - bytesToLong(buf)) / 1000);
			Thread.sleep(1000);
		}
	}

	public static byte[] longToBytes(long x) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(x);
		return buffer.array();
	}

	public static long bytesToLong(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(bytes);
		buffer.flip();// need flip
		return buffer.getLong();
	}
}
