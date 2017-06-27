package com.ctrip.xpipe.utils;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import com.ctrip.xpipe.metric.HostPort;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public class IpUtilsTest extends AbstractTest{
	
	@Test
	public void testGetFistNonLocalServerAddress(){
		
		logger.info("addr:{}", IpUtils.getFistNonLocalIpv4ServerAddress());
		logger.info("hostaddress:{}", IpUtils.getFistNonLocalIpv4ServerAddress().getHostAddress());
	}

	@Test
	public void testGetIp(){
		
		InetSocketAddress address = new InetSocketAddress("localhost", 6379);
		logger.info("{}", IpUtils.getIp(address));
		logger.info("{}", address.getAddress().getHostAddress());
	}

	@Test
	public void testParseAsHostPorts(){

		Assert.assertEquals(new LinkedList<>(), IpUtils.parseAsHostPorts(""));

	}
	

	@Test
	public void testIsLocal(){

		logger.info("{}", IpUtils.isLocal("127.0.0.1"));
		logger.info("{}", IpUtils.isLocal("10.32.21.2"));
		logger.info("{}", IpUtils.isLocal("10.32.21.3"));
		logger.info("{}", IpUtils.isLocal("0:0:0:0:0:0:0:1"));


	}

	@Test
	public void test(){

		IpUtils.getAllServerAddress().forEach((address) -> {

			if(address instanceof Inet6Address){

				logger.info("{}", address);
				logger.info("{}", address.getHostAddress());

				System.out.println();

			}
		});
	}


}
