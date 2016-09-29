package com.ctrip.xpipe.redis.integratedtest.consoleapi.util;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;

/**
 * @author liu
 * 
 *         Sep 9, 2016
 */
public class ApiTestExecitorPool extends AbstractExecutorPool {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private AtomicInteger successNum = new AtomicInteger(0);
	private AtomicLong successTotalDelay = new AtomicLong(0);
	private AtomicInteger failNum = new AtomicInteger(0);
	private AtomicLong failTotalDelay = new AtomicLong(0);

	private int threadNum = 1;
	private int threadExecutionNum= 100;
	private long threadSleepMsec = 100;

	private String url;
	private String apiName;

	@SuppressWarnings("rawtypes")
	private Class type;
	private Object defaultObj;
	private List<String> errorMessages = new Vector<String>();

	private CloseableHttpAsyncClient httpClient = HttpAsyncClients
			.createDefault();

	public boolean isOver = false;
	public boolean isPass = false;

	@SuppressWarnings("unused")
	private ApiTestExecitorPool() {
		super();
	}

	public ApiTestExecitorPool(String apiName, String url,
			@SuppressWarnings("rawtypes") Class type) {
		super();
		this.url = url;
		this.apiName = apiName;
		this.type = type;
	}

	public void doTest() {
		doTest(threadNum, threadExecutionNum, threadSleepMsec);
	}

	public void doTest(int threadNum, int threadExecutionNum, long threadSleepMsec) {
		this.threadNum = threadNum;
		this.threadExecutionNum = threadExecutionNum;
		this.threadSleepMsec = threadSleepMsec;
		httpClient.start();
		for (long i = 0; i < threadNum; i++) {
			this.addThread(getApiName());
		}
	}

	private Object initDefaultObj(Object obj) {
		if (defaultObj == null) {
			synchronized (super.fixedThreadPool) {
				if (defaultObj == null)
					defaultObj = obj;
			}
		}
		return defaultObj;
	}

	@Override
	protected void test() {
		HttpGet request = new HttpGet(url);
		for (int i = 0; i < threadExecutionNum; i++) {
			sendHttp(request);
			try {
				Thread.sleep(threadSleepMsec);
			} catch (InterruptedException e) {
				logger.error("InterruptedException", e);
			}
		}
	}

	public void handleHttpReturn(String result, long loseTime,
			String errorMessage) {
		boolean isOk = false;
		try {
			@SuppressWarnings("unchecked")
			Object obj = result == null ? null : DefaultSaxParser.parseEntity(
					type, result);
			if (obj != null && obj.equals(initDefaultObj(obj))) {
				isOk = true;
			}
		} catch (Exception e) {
			if (errorMessage == null)
				errorMessage = e.getMessage();
		} finally {
			int no = isOk ? addSuccess(loseTime) : addFail(loseTime);
			logger.info("{}--->{} ，{}[delay:{}ms]", no, getApiName(),
					isOk ? "success" : "failed", loseTime);
			if (errorMessage != null) {
				errorMessages
						.add(String
								.format("ErrorMessages%d--->%s ，Exception[delay:%dms,message:%s]",
										no, getApiName(), loseTime,
										errorMessage));
			}
			if (no >= this.threadNum * this.threadExecutionNum) {
				logger.info(
						"TOTAL--->{} ，success[count:{} , averageDelay:{}ms]，failed[count:{} , averageDelay:{}ms]",
						getApiName(), getSuccess(), getSucAverageDelay(),
						getFail(), getFaiAverageDelay());
				logger.info(
						"END--->{}>>>>============ BYEBYE ============<<<<",
						getApiName());
				try {
					httpClient.close();
				} catch (IOException e) {
					logger.error("[handleHttpReturn]", e);
				}
				fixedThreadPool.shutdown();
				if (getFail() > 0) {
					isPass = false;
				}else{
					isPass = true;
				}
				isOver = true;
			}
		}
	}

	private void sendHttp(HttpGet request) {

		httpClient.execute(request, new FutureCallback<HttpResponse>() {

			public long startTime;

			public void cancelled() {
			}

			public void completed(HttpResponse arg0) {
				String content = null;
				String errorMessage=null;
				try {
					content = EntityUtils.toString(arg0.getEntity(), "UTF-8");
				} catch (Exception e) {
					logger.error("[completed]",e);
					errorMessage=e.getMessage();
				}
				handleHttpReturn(content, System.currentTimeMillis()
						- startTime, errorMessage);
			}

			public void failed(Exception e) {
				handleHttpReturn(null, System.currentTimeMillis() - startTime,
						e.getMessage());
			}

			public FutureCallback<HttpResponse> setMystartTime(long startTime) {
				this.startTime = startTime;
				return this;
			}
		}.setMystartTime(System.currentTimeMillis()));
	}

	public void printErrorMessages() {
		logger.error(
				"ErrorMessages--->{}，start>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
				getApiName());
		for (String errorMessage : errorMessages) {
			logger.error(errorMessage);
		}
		logger.error(
				"ErrorMessages--->{}，end>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
				getApiName());
	}

	@Override
	protected int getPoolSize() {
		return 20;
	}

	protected String getUrl() {
		return this.url;
	}

	protected int addSuccess(long successDelay) {
		successTotalDelay.addAndGet(successDelay);
		return successNum.addAndGet(1);
	}

	protected int addFail(long failDelay) {
		failTotalDelay.addAndGet(failDelay);
		return failNum.addAndGet(1);
	}

	protected int getSuccess() {
		return successNum.get();
	}

	protected int getFail() {
		return failNum.get();
	}

	protected long getSucAverageDelay() {
		if (successNum.get() == 0)
			return 0;
		return successTotalDelay.get() / successNum.get();
	}

	protected long getFaiAverageDelay() {
		if (failNum.get() == 0)
			return 0;
		return failTotalDelay.get() / failNum.get();
	}

	public String getApiName() {
		return apiName;
	}

}
