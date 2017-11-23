package com.ctrip.xpipe.redis.integratedtest.console.consoleapi.util;

import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liu
 * 
 *         Sep 9, 2016
 */
public class ApiTestExecitorPool extends AbstractExecutorPool {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private AtomicInteger SUCCESS_NUM = new AtomicInteger(0);
	private AtomicInteger FAIL_NUM = new AtomicInteger(0);
	private int testNum;
	private String url;
	private String apiName;
	private RestOperations restTemplate = RestTemplateFactory
			.createCommonsHttpRestTemplate(10, 100, 5000, 5000);

	@SuppressWarnings("rawtypes")
	private Class type;

	@SuppressWarnings("unused")
	private ApiTestExecitorPool() {
		super();
	}

	public ApiTestExecitorPool(String apiName, String url,
			@SuppressWarnings("rawtypes") Class type, int testNum) {
		super();
		this.url = url;
		this.testNum = testNum;
		this.apiName = apiName;
		this.type = type;
	}

	public void doTest() {
		for (int i = 1; i <= testNum; i++) {
			this.addThread(getApiName());
		}
	}

	@Override
	protected void test() {
		boolean isOk = false;
		try {
			String result = restTemplate.getForObject(url, String.class);
			@SuppressWarnings("unchecked")
			Object obj = DefaultSaxParser.parseEntity(type, result);
			if (obj != null)
				isOk = true;
		} catch (Exception e) {
			logger.error("Exception", e);
		} finally {
			if (isOk) {
				addSuccess();
			} else {
				addFail();
			}
			if ((getSuccess() + getFail()) == getTestNum()) {
				logger.error("{} ，success:{}，fail:{}", getApiName(),
						getSuccess(), getFail());
				fixedThreadPool.shutdown();
			}
		}
	}

	protected int getTestNum() {
		return this.testNum;
	}

	@Override
	protected int getPoolSize() {
		return 5;
	}

	protected String getUrl() {
		return this.url;
	}

	protected void addSuccess() {
		SUCCESS_NUM.addAndGet(1);
	}

	protected void addFail() {
		FAIL_NUM.addAndGet(1);
	}

	protected int getSuccess() {
		return SUCCESS_NUM.get();
	}

	protected int getFail() {
		return FAIL_NUM.get();
	}

	public String getApiName() {
		return apiName;
	}
}
