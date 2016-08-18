/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.netty;


import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

/**
 * @author marsqing
 *
 *         Jul 28, 2016 2:59:57 PM
 */
public class ChannelUtil {

	private final static Logger logger = LoggerFactory.getLogger(ChannelUtil.class);

	public static String getRemoteIpLocalPort(Channel channel) {
		String remoteIpLocalPort = "unknown";

		try {
			String remoteIp = ((InetSocketAddress) channel.remoteAddress()).getAddress().getHostAddress();
			int localPort = ((InetSocketAddress) channel.localAddress()).getPort();
			return remoteIp + "->" + localPort;
		} catch (Exception e) {
			logger.warn("Error parse remote ip and local port from Channel {}", channel);
		}

		return remoteIpLocalPort;
	}

	public static String getRemoteAddr(Channel channel) {
		String remoteIpLocalPort = "unknown";
		try {
			InetSocketAddress remoteAddr = (InetSocketAddress)channel.remoteAddress();
			return String.format("R(%s:%d)", remoteAddr.getHostName(), remoteAddr.getPort());
		} catch (Exception e) {
			logger.warn("Error parse remote ip and local port from Channel {}", channel);
		}
		return remoteIpLocalPort;
	}

}
