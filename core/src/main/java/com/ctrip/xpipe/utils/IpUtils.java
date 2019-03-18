package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *
 * 2016年4月26日 下午5:32:48
 */
public class IpUtils {

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
		return ip != null && com.google.common.net.InetAddresses.isInetAddress(ip);
	}
	
	public static boolean isPort(String str){
		
		try{
			int port = Integer.parseInt(str);
			if(port >= 0 && port <= 65535){
				return true;
			}
		}catch(Exception e){
		}
		return false;
	}

	public static InetAddress getFistNonLocalIpv4ServerAddress() {
		FoundationService.DEFAULT.getLocalIp();
		return getFistNonLocalIpv4ServerAddress("10");
	}

	public static InetAddress getFistNonLocalIpv4ServerAddress(String ipPrefixPrefer) {

		InetAddress first = null;

		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			if (interfaces == null) {
				return null;
			}
			while (interfaces.hasMoreElements()) {
				NetworkInterface current = interfaces.nextElement();
				if (current.isLoopback()) {
					continue;
				}
				List<InterfaceAddress> addresses = current.getInterfaceAddresses();
				if (addresses.size() == 0) {
					continue;
				}
				for (InterfaceAddress interfaceAddress : addresses) {
					InetAddress address = interfaceAddress.getAddress();
					if (address instanceof Inet4Address) {
						if(first == null){
							first = address;
						}
						if(address.getHostAddress().startsWith(ipPrefixPrefer)){
							return address;
						}
					}
				}
			}
		} catch (SocketException e) {
		}

		if(first != null){
			return first;
		}
		throw new IllegalStateException("[can not find a qualified address]");
	}


	public static boolean isLocal(String host){

		if(host.startsWith("/")){
			host = host.substring(1);
		}

		for(InetAddress address : getAllServerAddress()){

			logger.debug("{}", address.getHostAddress());
			if(host.equalsIgnoreCase(getAddressString(getAddressString(address.getHostAddress())))){
				return true;
			}
		}
		return  false;
	}

	private static String getAddressString(String hostAddress) {
		//for ipv6
		int index = hostAddress.indexOf("%");
		if(index >= 0){
			hostAddress = hostAddress.substring(0, index);
		}
		return hostAddress;
	}

	protected static List<InetAddress> getAllServerAddress() {

		List<InetAddress> result = new LinkedList<>();

		try {
			Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
			if (interfaces == null) {
				return null;
			}
			while (interfaces.hasMoreElements()) {
				NetworkInterface current = interfaces.nextElement();
				List<InterfaceAddress> addresses = current.getInterfaceAddresses();
				if (addresses.size() == 0) {
					continue;
				}
				for (InterfaceAddress interfaceAddress : addresses) {
					InetAddress address = interfaceAddress.getAddress();
					result.add(address);
				}
			}
		} catch (SocketException e) {
		}
		return result;
	}

	public static List<HostPort> parseAsHostPorts(String addressDesc){

		if(addressDesc == null){
			throw new IllegalArgumentException("addressDesc null");
		}

		if(StringUtil.isEmpty(addressDesc)){
			return new LinkedList<>();
		}

		List<HostPort> result = new LinkedList<>();
		String []addresses = addressDesc.split("\\s*,\\s*");

		for(String address : addresses){

			try {
				HostPort hostPort = parseSingleAsHostPort(address);
				result.add(hostPort);
			} catch (Exception e) {
				logger.warn("[parse][wrong address]" + address);
			}
		}
		return result;
	}

	private static HostPort parseSingleAsHostPort(String singleAddress) {

		Pair<String, Integer> pair = parseSingleAsPair(singleAddress);
		return new HostPort(pair.getKey(), pair.getValue());
	}


	public static List<InetSocketAddress> parse(String addressDesc){

		List<HostPort> hostPorts = parseAsHostPorts(addressDesc);

		List<InetSocketAddress> result = new LinkedList<>();

		hostPorts.forEach((hostPort) -> {
			result.add(new InetSocketAddress(hostPort.getHost(), hostPort.getPort()));

		});
		return result;
	}
	
	public static InetSocketAddress parseSingle(String singleAddress){

		Pair<String, Integer> pair = parseSingleAsPair(singleAddress);
		return new InetSocketAddress(pair.getKey(), pair.getValue());
	}
	
	public static Pair<String, Integer> parseSingleAsPair(String singleAddress){
		
		String []parts = singleAddress.split("\\s*:\\s*");
		if(parts.length != 2){
			throw new IllegalArgumentException("invalid socket address:" + singleAddress);
		}
		return new Pair<>(parts[0], Integer.parseInt(parts[1]));
	}

	public static String[] splitIpAddr(String ip) {
		return StringUtil.splitRemoveEmpty("\\s*\\.\\s*", ip);
	}
}
