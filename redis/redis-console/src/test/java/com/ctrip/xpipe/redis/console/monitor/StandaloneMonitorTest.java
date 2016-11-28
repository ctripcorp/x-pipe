package com.ctrip.xpipe.redis.console.monitor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

import com.ctrip.xpipe.redis.console.monitor.impl.StandaloneStatMonitor;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;

public class StandaloneMonitorTest extends AbstractRedisTest{
	@Test
	public void standAloneMonitorTest() throws UnsupportedEncodingException, IOException {
		new StandaloneStatMonitor("standalone-stat-monitor-test.json");
		waitForAnyKeyToExit();
	}
	
}
