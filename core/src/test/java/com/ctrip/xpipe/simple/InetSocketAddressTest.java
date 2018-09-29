package com.ctrip.xpipe.simple;

import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * @author shyin
 *
 * Dec 22, 2016
 */
public class InetSocketAddressTest {
	@Test
	public void test() {
		System.out.println(new InetSocketAddress("0.0.0.0", 0));
		System.out.println(InetSocketAddress.createUnresolved("0.0.0.0", 0));
	}
}
