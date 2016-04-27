package com.ctrip.xpipe.utils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author wenchao.meng
 *
 * 2016年4月26日 下午5:32:48
 */
public class IpUtils {
	
	public static String getIp(SocketAddress socketAddress){
		
		
		if(socketAddress instanceof InetSocketAddress){
			
			InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
			String ip = inetSocketAddress.getAddress().toString(); 
			if(ip.charAt(0) == '/'){
				return ip.substring(1);
			}
			return ip;
		}
		
		throw new IllegalStateException("unknown socketaddress type:" + socketAddress.getClass() + "," + socketAddress);
	}
}
