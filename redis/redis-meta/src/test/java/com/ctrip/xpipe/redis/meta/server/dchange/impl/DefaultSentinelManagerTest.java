package com.ctrip.xpipe.redis.meta.server.dchange.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.DefaultSentinelManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Dec 11, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultSentinelManagerTest extends AbstractMetaServerTest{
	
	private DefaultSentinelManager sentinelManager;
	
	private String sentinelMonitorName = "mymaster";
	
	private List<String> sentinels;

	private List<Server> servers;
	
	private ExecutionLog executionLog;
	
	private RedisMeta redisMaster;
	
	private int port = 6379;

	private BiFunction<Integer, String, String> requestHandler;

	private static final String SENTINEL_INFO_TEMPLATE = "*26\r\n" +
			"$4\r\nname\r\n" + "$%d\r\n%s\r\n" +
			"$2\r\nip\r\n" + "$%d\r\n%s\r\n" +
			"$4\r\nport\r\n" + "$%d\r\n%s\r\n" +
			"$5\r\nrunid\r\n" + "$40\r\nc6831f23150c7bcb28a86534ae1f55a4a3b9068e\r\n" + "$5\r\nflags\r\n$" + "8\r\nsentinel\r\n" +
			"$16\r\npending-commands\r\n" + "$1\r\n0\r\n" + "$14\r\nlast-ping-sent\r\n" + "$1\r\n0\r\n" +
			"$18\r\nlast-ok-ping-reply\r\n" + "$3\r\n542\r\n" + "$15\r\nlast-ping-reply\r\n" + "$3\r\n542\r\n" +
			"$23\r\ndown-after-milliseconds\r\n" + "$5\r\n30000\r\n" + "$18\r\nlast-hello-message\r\n" + "$3\r\n203\r\n" +
			"$12\r\nvoted-leader\r\n" + "$1\r\n?\r\n" + "$18\r\nvoted-leader-epoch\r\n" + "$1\r\n0\r\n";

	@Mock
	private DcMetaCache dcMetaCache;
	
	@Before
	public void beforeDefaultSentinelManagerTest() throws Exception{
        DefaultSentinelManager.DEFAULT_MIGRATION_SENTINEL_COMMAND_WAIT_TIMEOUT_MILLI = 2000;
		sentinelManager = new DefaultSentinelManager(dcMetaCache, getXpipeNettyClientKeyedObjectPool());
		executionLog = new ExecutionLog(currentTestName());
		redisMaster = new RedisMeta().setIp("127.0.0.1").setPort(port);

		servers = new ArrayList<>();
		sentinels = new ArrayList<>();
		requestHandler = null;
		IntStream.range(0, 5).forEach(i -> {
			try {
				Server server = startServer(randomPort(), new Function<String, String>() {
					@Override
					public String apply(String s) {
						if (null == requestHandler) return "+OK\r\n";
						else return requestHandler.apply(i, s);
					}
				});
				servers.add(server);
				sentinels.add("127.0.0.1:" + server.getPort());
			} catch (Exception e) {
				logger.debug("init server fail {}", e.getMessage());
			}
		});

		when(dcMetaCache.getSentinelMonitorName(getClusterDbId(), getShardDbId())).thenReturn(sentinelMonitorName);
		when(dcMetaCache.getSentinel(getClusterDbId(), getShardDbId())).thenReturn(new SentinelMeta().setAddress(String.join(",", sentinels)));
	}

	@After
	public void afterDefaultSentinelManagerTest() throws Exception {
		if (null != servers) {
			servers.forEach(server -> {
				try {
					server.stop();
				} catch (Exception e) {
				}
			});
		}
	}

	@Test
	public void testRemove() throws TimeoutException {
		AtomicInteger removeCnt = new AtomicInteger(0);

		requestHandler = new BiFunction<Integer, String, String>() {
			@Override
			public String apply(Integer source, String s) {
				if (null != s && s.equals("sentinel sentinels " + sentinelMonitorName)) {
					return buildSentinelsResponse(source);
				} else if (null != s && s.equals("sentinel remove " + sentinelMonitorName)) {
					removeCnt.incrementAndGet();
				}
				return "+OK\r\n";
			}
		};

		sentinelManager.removeSentinel(getClusterDbId(), getShardDbId(), executionLog);
		logger.info("remove: {}", executionLog.getLog());
		waitConditionUntilTimeOut(()->removeCnt.get() == sentinels.size());
	}

	@Test
	public void testAdd(){
		
		sentinelManager.addSentinel(getClusterDbId(), getShardDbId(), new HostPort(redisMaster.getIp(), redisMaster.getPort()), executionLog);
		logger.info("addSuccess: {}", executionLog.getLog());
		servers.forEach(server -> {
			Assert.assertTrue(executionLog.getLog().contains(String.format("add %s %s:%d 3 to sentinel redis://%s:%d", sentinelMonitorName, LOCAL_HOST, port, LOCAL_HOST, server.getPort())));
		});
	}

	@Test
	public void testAddFailed() throws Exception {
		servers.get(0).stop();
		try {
			sentinelManager.addSentinel(getClusterDbId(), getShardDbId(), new HostPort(redisMaster.getIp(), redisMaster.getPort()), executionLog);
			logger.info("addFailed: {}", executionLog.getLog());
			servers.forEach(server -> {
				Assert.assertTrue(executionLog.getLog().contains(String.format("add %s %s:%d 3 to sentinel redis://%s:%d", sentinelMonitorName, LOCAL_HOST, port, LOCAL_HOST, server.getPort())));
			});
		} catch (Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void testEmpty(){
		
		when(dcMetaCache.getSentinel(getClusterDbId(), getShardDbId())).thenReturn(new SentinelMeta().setAddress(""));
		
		sentinelManager.removeSentinel(getClusterDbId(), getShardDbId(), executionLog);
		logger.info("{}", executionLog.getLog());
		
	}

	@Test
	public void testRemoveSingleSentinel() throws Exception {
		AtomicInteger removeCnt = new AtomicInteger(0);
		requestHandler = new BiFunction<Integer, String, String>() {
			@Override
			public String apply(Integer integer, String s) {
				if (null != s && s.equals("sentinel sentinels " + sentinelMonitorName)) {
					return "*0\r\n";
				} else if (null != s && s.equals("sentinel remove " + sentinelMonitorName)) {
					removeCnt.incrementAndGet();
				}
				return "+OK\r\n";
			}
		};
		sentinelManager.removeSentinel(getClusterDbId(), getShardDbId(), executionLog);
		logger.info("test result {}", executionLog.getLog());
		Assert.assertEquals(removeCnt.get(), 5);
	}

	@Test
	public void testRemovedFailed() throws Exception {
		AtomicInteger removeCnt = new AtomicInteger(0);

		requestHandler = new BiFunction<Integer, String, String>() {
			@Override
			public String apply(Integer source, String s) {
				if (null != s && s.equals("sentinel sentinels " + sentinelMonitorName)) {
					return buildSentinelsResponse(source);
				} else if (null != s && s.equals("sentinel remove " + sentinelMonitorName)) {
					removeCnt.incrementAndGet();
				}
				return "+OK\r\n";
			}
		};

        servers.get(0).stop();
        try {
            sentinelManager.removeSentinel(getClusterDbId(), getShardDbId(), executionLog);
            logger.info("test result {}", executionLog.getLog());
            servers.forEach(server -> {
                Assert.assertTrue(executionLog.getLog().contains(String.format("removeSentinel %s from redis://%s:%d", sentinelMonitorName, LOCAL_HOST, server.getPort())));
            });
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();

        }
	}

	private String buildSentinelsResponse(int source) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("*").append(sentinels.size() - 1).append("\r\n");

		IntStream.range(0, sentinels.size()).forEach(i -> {
			if (source == i) return;
			String sentinel = sentinels.get(i);
			String[] address = sentinel.split(":");
			stringBuilder.append(String.format(SENTINEL_INFO_TEMPLATE,
					sentinel.length(), sentinel, address[0].length(), address[0], address[1].length(), address[1]));
		});

		return stringBuilder.toString();
	}

}
