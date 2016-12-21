package com.ctrip.xpipe.redis.console.health.rawtcp;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author marsqing
 *
 *         Dec 9, 2016 4:02:07 PM
 */
public class Server {

	public static void main(String[] args) throws Exception {
		@SuppressWarnings("resource")
		ServerSocket ss = new ServerSocket(7777);

		while (true) {
			Socket s = ss.accept();

			process(s);
		}
	}

	private static void process(Socket s) throws Exception {
		s.setTcpNoDelay(true);
		InputStream in = s.getInputStream();
		OutputStream out = s.getOutputStream();
		byte[] buf = new byte[8];
		while (true) {
			in.read(buf);
			out.write(buf);
		}
	}

}
