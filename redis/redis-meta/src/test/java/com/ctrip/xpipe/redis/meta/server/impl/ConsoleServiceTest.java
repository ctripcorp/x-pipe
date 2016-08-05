package com.ctrip.xpipe.redis.meta.server.impl;

import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.meta.server.AbstractIntegrationTest;
import com.ctrip.xpipe.redis.meta.server.AppTest;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ConsoleServiceTest extends AppTest{

	@Autowired
	private ConsoleService consoleService;

	@Test
	public void test(){
		consoleService.getClusterMeta("jq", "cluster1");
	}

}
