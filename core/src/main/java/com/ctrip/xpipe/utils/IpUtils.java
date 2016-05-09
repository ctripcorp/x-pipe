package com.ctrip.xpipe.utils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.regex.Pattern;

/**
 * @author wenchao.meng
 *
 * 2016年4月26日 下午5:32:48
 */
public class IpUtils {
	
	private static Pattern IP_PATTERN = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");
	
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
	
	public static boolean isValidIpFormat(String ip) {
		return ip != null && IP_PATTERN.matcher(ip).matches();
	}
}
