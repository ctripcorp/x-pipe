package com.ctrip.xpipe.utils;

import java.io.IOException;
import java.net.Socket;

/**
 * @author wenchao.meng
 *
 * Sep 25, 2016
 */
public class TcpPortCheck {
	
	private String host;
	private int port;
	
	public TcpPortCheck(String host, int port){
		this.host = host;
		this.port = port;
	}
	
	public boolean checkOpen(){
		try (Socket socket = new Socket(host, port)){
			return true;
		} catch (IOException e) {
			return false;
		}
	}

}
