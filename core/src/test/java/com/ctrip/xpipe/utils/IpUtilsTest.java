package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.LinkedList;

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

	@Test
	public void testIpSplit() {
		String ip = "10";
		Assert.assertEquals(1, IpUtils.splitIpAddr(ip).length);
		Assert.assertEquals(ip, IpUtils.splitIpAddr(ip)[0]);

		ip = "10.26";
		Assert.assertEquals(2, IpUtils.splitIpAddr(ip).length);
		Assert.assertEquals("10", IpUtils.splitIpAddr(ip)[0]);
		Assert.assertEquals("26", IpUtils.splitIpAddr(ip)[1]);
	}

	@Test
	public void testIsValidIp() {
		String ipv4 = "192.168.0.1";
		Assert.assertTrue(IpUtils.isValidIpFormat(ipv4));

		String ipv6 = "fe80::1ff:fe23:4567:890a";
		Assert.assertTrue(IpUtils.isValidIpFormat(ipv6));

		ipv6 = "fe80::1ff:fe23:4567:890a";
		Assert.assertTrue(IpUtils.isValidIpFormat(ipv6));
	}


}
