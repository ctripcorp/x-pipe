package com.ctrip.xpipe.redis.keeper.simple;

import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.simpleserver.Server;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Function;

/**
 * @author wenchao.meng
 *
 *         May 18, 2016 4:44:03 PM
 */
public class SimpleTest extends AbstractRedisTest {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Test
	public void testServer() throws Exception {

		Server server = startServer(6379, new Function<String, String>() {
			@Override
			public String apply(String request) {

				if(request == null){
					return null;
				}

				if(request.equalsIgnoreCase("PING")){
					return "+PONG\r\n";
				}

				if(request.indexOf("SYNC") >= 0){
					return "-ERRORXX\r\n";
				}
				return "+OK\r\n";
			}
		});

		waitForAnyKey();
	}

	@Test
	public void test() throws FileNotFoundException {

		File f = new File("/opt/logs/test");
		logger.info("[exist]{}", f.exists());

	}

	@Test
	public void testFormat() {

		logger.info("nihaoma");

		System.out.println(String.format("%,d", 111111111));
		System.out.println(String.format("%1$s", "a b c"));
		System.out.println(String.format("%s", "a b c"));
		// System.out.println(String.format("%2$tm %2$te,%2$tY",
		// Calendar.getInstance()));

		Calendar calendar = Calendar.getInstance();
		System.out.println(calendar);
		// logger.printf(Level.INFO, "%2$tm %2$te,%2$tY", calendar);

	}

	@Test
	public void testJson() {

		ReplicationStoreMeta meta = new ReplicationStoreMeta();
		meta.setBeginOffset(100L);

		String json = JSON.toJSONString(meta);

		System.out.println(json);

		meta = JSON.parseObject(null, ReplicationStoreMeta.class);
		System.out.println(meta);

	}

	@Test
	public void testCat() throws IOException {

		Transaction t1 = Cat.newTransaction("type1", "name1");
		Transaction t21 = Cat.newTransaction("type21", "name2");
		Transaction t31 = Cat.newTransaction("type31", "name3");
		t31.setStatus(Transaction.SUCCESS);
		t31.complete();
		t21.setStatus(Transaction.SUCCESS);
		t21.complete();

		Transaction t22 = Cat.newTransaction("type22", "name2");
		t22.setStatus(Transaction.SUCCESS);
		t22.complete();
		t1.setStatus(Transaction.SUCCESS);
		t1.complete();

		waitForAnyKeyToExit();
	}

	@Test
	public void testPort() throws Exception {

		for (int i = 0; i < 10; i++) {
			Server server = startEchoServer();
			Assert.assertFalse(isUsable(server.getPort()));
		}
	}

	@Test
	public void testHandler() {

		System.out.println("".split("\\s+").length);
	}

	@Test
	public void simpleTest() throws Exception {

		int port = 1111;
		startEchoServer(port);

		logger.info("any host");
		try {
			Socket socket = new Socket();
			SocketAddress target = new InetSocketAddress(port);
			logger.info("{}", target);
			socket.connect(target);
			logger.info("{}", socket);
		} catch (Exception e) {
			logger.error("[simpleTest]", e);
		}
	}

	@Test
	public void testInet() throws SocketException {
		
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		if (interfaces == null) {
			return ;
		}
		while (interfaces.hasMoreElements()) {
			NetworkInterface current = interfaces.nextElement();
			List<InterfaceAddress> addresses = current.getInterfaceAddresses();
			if (addresses.size() == 0) {
				continue;
			}
			for (InterfaceAddress interfaceAddress : addresses) {
				InetAddress address = interfaceAddress.getAddress();
				logger.info("{}", address);
			}
		}
	}

}
