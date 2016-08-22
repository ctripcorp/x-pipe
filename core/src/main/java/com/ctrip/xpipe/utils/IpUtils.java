package com.ctrip.xpipe.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * 2016年4月26日 下午5:32:48
 */
public class IpUtils {
	
	private static Pattern IP_PATTERN = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");
	private static Logger logger = LoggerFactory.getLogger(IpUtils.class);
	
	public static String getIp(SocketAddress socketAddress){
		
		
		if(socketAddress instanceof InetSocketAddress){
			
			InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
			String ip = inetSocketAddress.getAddress().getHostAddress(); 
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
			if(interfaces == null){
				return null;
			}
			while(interfaces.hasMoreElements()){
				 NetworkInterface current = interfaces.nextElement();
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
	
	public static List<InetSocketAddress> parse(String addressDesc){
		
		List<InetSocketAddress> result = new LinkedList<>();
		String []addresses = addressDesc.split("\\s*,\\s*");
		for(String address : addresses){

			try {
				InetSocketAddress inetAddress = parseSingle(address);
				result.add(inetAddress);
			} catch (Exception e) {
				logger.warn("[parse][wrong address]" + address);
			}
		}
		return result;
	}
	
	public static InetSocketAddress parseSingle(String singleAddress) throws Exception{

		String []parts = singleAddress.split("\\s*:\\s*");
		if(parts.length != 2){
			throw new Exception("invalid socket address:" + singleAddress);
		}
		return new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
	} 
}
