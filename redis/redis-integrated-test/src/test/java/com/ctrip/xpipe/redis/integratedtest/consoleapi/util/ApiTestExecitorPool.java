package com.ctrip.xpipe.redis.integratedtest.consoleapi.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

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
@SuppressWarnings("deprecation")
public class ApiTestExecitorPool extends AbstractExecutorPool{

	private final static Logger logger = LoggerFactory
			.getLogger(ApiTestExecitorPool.class);
	private AtomicInteger successNum = new AtomicInteger(0);
	private AtomicLong successTotalDelay = new AtomicLong(0);
	private AtomicInteger failNum = new AtomicInteger(0);
	private AtomicLong failTotalDelay = new AtomicLong(0);

	private int threadNum = 1;
	private int threadExecutionNum = 100;
	private long threadSleepMsec = 100;

	private String url;
	private String apiName;

	private CountDownLatch latch;

	@SuppressWarnings("rawtypes")
	private Class type;
	private Object defaultObj;

	private CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
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

	public void doTest(int threadNum, int threadExecutionNum,
			long threadSleepMsec) {
		latch = new CountDownLatch(threadNum * threadExecutionNum);
		this.threadNum = threadNum;
		this.threadExecutionNum = threadExecutionNum;
		this.threadSleepMsec = threadSleepMsec;
		httpClient.start();
		for (long i = 0; i < threadNum; i++) {
			this.addThread(getApiName());
		}
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("[doTest][await]InterruptedException", e);
		} finally {
			close();
			logger.info("test is over");
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
			try {
				sendHttp(request);
			} catch (Exception e) {
				logger.error("[test][sendHttp]Exception", e);
				latch.countDown();
			} finally {
				try {
					Thread.sleep(threadSleepMsec);
				} catch (InterruptedException e) {
					logger.error("[test] sleep exception", e);
				}
			}
		}
	}

	@SuppressWarnings("deprecation")
	public void handleHttpReturn(String result, long loseTime,
			Exception httpException) {
		if (httpException != null) {
			logger.error("[sendHttp]httpException", httpException);
		}
		Assert.assertNull("HttpReturn Exception", httpException);
		boolean isOk = false;
		try {
			@SuppressWarnings("unchecked")
			Object obj = result == null ? null : DefaultSaxParser.parseEntity(
					type, result);
			if (obj != null && obj.equals(initDefaultObj(obj))) {
				isOk = true;
			}
			Assert.assertEquals("The two result is not the same!",
					initDefaultObj(obj), obj);
		} catch (Exception e) {
			logger.error("[handleHttpReturn][exception]", e);
		} finally {
			int no = isOk ? addSuccess(loseTime) : addFail(loseTime);
			logger.info("{}--->{} ，{}[delay:{}ms]", no, getApiName(),isOk ? "success" : "failed", loseTime);
			if (no >= this.threadNum * this.threadExecutionNum) {
				logger.info("TOTAL--->{} ，success[count:{} , averageDelay:{}ms]，failed[count:{} , averageDelay:{}ms]",
						getApiName(), getSuccess(), getSucAverageDelay(),getFail(), getFaiAverageDelay());
				isPass = true;
			}
			latch.countDown();
		}
	}

	private void close() {
		if (httpClient != null) {
			try {
				httpClient.close();
			} catch (Exception e) {
				logger.error("[close]close httpClient Exception", e);
			}
		}
		if (fixedThreadPool != null) {
			try {
				fixedThreadPool.shutdown();
			} catch (Exception e) {
				logger.error("[close]shutdown threadPool Exception", e);
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
				try {
					content = EntityUtils.toString(arg0.getEntity(), "UTF-8");
				} catch (Exception e) {
					logger.error("[sendHttp][completed][Exception]", e);
					handleHttpReturn(content, System.currentTimeMillis()-startTime, e);
				}
				handleHttpReturn(content, System.currentTimeMillis()-startTime, null);
			}

			public void failed(Exception e) {
				logger.error("[sendHttp][failed][Exception]", e);
				handleHttpReturn(null, System.currentTimeMillis() - startTime,e);
			}

			public FutureCallback<HttpResponse> setMystartTime(long startTime) {
				this.startTime = startTime;
				return this;
			}
		}.setMystartTime(System.currentTimeMillis()));
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

	@Override
	protected void threadExceptionHandler(Runnable r, Throwable t) {
		logger.error("[threadExceptionHandler]",t);
	}

	

}
