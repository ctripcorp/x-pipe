package com.ctrip.xpipe.redis.integratedtest.consoleapi;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.integratedtest.consoleapi.util.ApiTestExecitorPool;

/**
 * @author liuyi
 * 
 *         Sep 9, 2016
 */

@RunWith(Parameterized.class)
public class ConsoleApiTest extends AbstractTest {
	final static private long THREAD_MAX_RUNTIME = 100000;
	private int threadNum, threadExecutionNum;
	private long threadSleepMsec;
	private String apiName;
	private String apiUrl;
	private Class type;

	public ConsoleApiTest(int threadNum, int threadExecutionNum,
			long threadSleepMsec, String apiName, String apiUrl, Class type) {
		this.threadNum = threadNum;
		this.threadExecutionNum = threadExecutionNum;
		this.threadSleepMsec = threadSleepMsec;
		this.apiName = apiName;
		this.apiUrl = apiUrl;
		this.type = type;
	}

	@Parameters
	public static Collection prepareData() {
		Object[][] object = {
				{ 1, 10, 100, "apiName1", "apiUrl1", ClusterMeta.class },
				{ 1, 10, 100, "apiName2", "apiUrl2", ClusterMeta.class } };
		return Arrays.asList(object);
	}

	@Test(timeout = THREAD_MAX_RUNTIME)
	public void apiTest() {
		ApiTestExecitorPool api = new ApiTestExecitorPool(apiName, apiUrl,
				ClusterMeta.class);
		api.doTest(threadNum, threadExecutionNum, threadSleepMsec);
		assertTrue(api.isPass);
	}
}
