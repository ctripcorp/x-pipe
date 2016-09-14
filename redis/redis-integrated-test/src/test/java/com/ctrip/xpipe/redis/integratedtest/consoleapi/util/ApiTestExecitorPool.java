package com.ctrip.xpipe.redis.integratedtest.consoleapi.util;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.spring.RestTemplateFactory;

/**
 * @author liu
 * 
 *         Sep 9, 2016
 */
public class ApiTestExecitorPool extends AbstractExecutorPool {

	private Logger logger = LoggerFactory.getLogger(getClass());
	private AtomicInteger successNum = new AtomicInteger(0);
	private AtomicLong successTotalDelay = new AtomicLong(0);
	private AtomicInteger failNum= new AtomicInteger(0);
	private AtomicLong failTotalDelay = new AtomicLong(0);
	// default 1000 qps
	private long qps = 1000;
	// default 1 second
	private long timeSeconds = 1;
	private String url;
	private String apiName;
	private RestTemplate restTemplate = RestTemplateFactory
			.createCommonsHttpRestTemplate(10, 100, 5000, 5000);
	private Object defaultObj;
	private List<String>errorMessages=new Vector<String>();
	
	@SuppressWarnings("rawtypes")
	private Class type;

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
		doTest(qps,timeSeconds);
	}

	public void doTest(long qps) {
		this.setQps(qps);
		doTest(qps,timeSeconds);
	}

	public void addThread() {
		for (int i = 1; i <= getQps(); i++) {
			this.addThread(getApiName());
		}
	}

	public void doTest(long qps, long timeSeconds) {
		this.setQps(qps);
		this.setTimeSeconds(timeSeconds);
		for (long i = 1; i <= timeSeconds; i++) {
			this.addThread();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void setQps(long qps) {
		this.qps = qps;
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
		String errorMessage=null;
		boolean isOk = false;
		long start = System.currentTimeMillis();
		try {
			String result = restTemplate.getForObject(url, String.class);
			@SuppressWarnings("unchecked")
			Object obj = DefaultSaxParser.parseEntity(type, result);
			if (obj != null && obj.equals(initDefaultObj(obj))) {
				isOk = true;
			} else {
				throw new RuntimeException("return Objects are  different");
			}
		} catch (Exception e) {
			errorMessage=e.getMessage();
		} finally {
			long losetime=System.currentTimeMillis() - start;
			int no=isOk?addSuccess(losetime):addFail(losetime);
			logger.error("{}--->{} ，{}[delay:{}ms]", no,getApiName(), isOk?"success":"failed",losetime);
			if(errorMessage!=null){
				errorMessages.add(String.format("ErrorMessages%d--->%s ，Exception[delay:%dms,message:%s]", no, getApiName(),losetime,errorMessage));
			}
			if (no>= (getQps() * getTimeSeconds())) {
				printErrorMessages();
				logger.error("TOTAL--->{} ，success[count:{} , averageDelay:{}ms]，failed[count:{} , averageDelay:{}ms]", getApiName(),
						getSuccess(),getSucAverageDelay(),getFail(),getFaiAverageDelay());
				fixedThreadPool.shutdown();
			}
		}
	}

	private void printErrorMessages() {
		logger.error("ErrorMessages--->{}，start>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",getApiName());
		for(String errorMessage:errorMessages){
			logger.error(errorMessage);
		}
		logger.error("ErrorMessages--->{}，end>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",getApiName());
	}

	protected long getQps() {
		return this.qps;
	}

	@Override
	protected int getPoolSize() {
		return 10000;
	}

	protected String getUrl() {
		return this.url;
	}

	protected int addSuccess(long successDelay) {
		successTotalDelay.addAndGet(successDelay);
		return  successNum.addAndGet(1);
	}

	protected int addFail(long failDelay) {
		failTotalDelay.addAndGet(failDelay);
		return  failNum.addAndGet(1);
	}

	protected int getSuccess() {
		return successNum.get();
	}

	protected int getFail() {
		return failNum.get();
	}

	protected long getSucAverageDelay(){
		if(successNum.get()==0)
			return 0;
		return successTotalDelay.get()/successNum.get();
	}
	
	protected long getFaiAverageDelay(){
		if(failNum.get()==0)
			return 0;
		return failTotalDelay.get()/failNum.get();
	}
	private String getApiName() {
		return apiName;
	}

	private long getTimeSeconds() {
		return timeSeconds;
	}

	private void setTimeSeconds(long timeSeconds) {
		this.timeSeconds = timeSeconds;
	}

}
