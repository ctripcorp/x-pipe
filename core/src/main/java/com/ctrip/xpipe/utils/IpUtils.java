package com.ctrip.xpipe.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
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
	
	public static boolean isPort(String str){
		
		try{
			int port = Integer.parseInt(str);
			if(port > 0 && port <= 65535){
				return true;
			}
		}catch(Exception e){
		}
		return false;
	}
	
	public static InetAddress getFistNonLocalIpv4ServerAddress(){
		
		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			while(interfaces.hasMoreElements()){
				 NetworkInterface current = interfaces.nextElement();
//				 System.out.println(current);
//				 System.out.println(current.getInterfaceAddresses());
				 if(current.isLoopback()){
					 continue;
				 }
				 List<InterfaceAddress> addresses = current.getInterfaceAddresses();
				 if(addresses.size() == 0){
					 continue;
				 }
				 for(InterfaceAddress interfaceAddress : addresses){
					InetAddress address = interfaceAddress.getAddress();
					 if(address instanceof Inet4Address){
						 return address;
					 }
				 }
			}
		} catch (SocketException e) {
		}
		
		throw new IllegalStateException("[can not find a qualified address]");
	}
}
