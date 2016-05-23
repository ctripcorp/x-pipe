package com.ctrip.xpipe;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.exception.DefaultExceptionHandler;
import com.ctrip.xpipe.utils.OsUtils;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:44:47
 */
public class AbstractTest {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ExecutorService executors = Executors.newCachedThreadPool();

	protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(OsUtils.getCpuCount());

	@Rule
	public TestName name = new TestName();
	
	private Properties properties = new Properties();
	
	@Before
	public void beforeAbstractTest() throws IOException{
		
		logger.info("[begin test]" + name.getMethodName());
		
		Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());
		InputStream fins = getClass().getClassLoader().getResourceAsStream("xpipe-test.properties");
		try {
			properties.load(fins);
		} finally{
			if(fins != null){
				fins.close();
			}
		}
		
		File file = new File(getTestFileDir());
		if(file.exists()){
			FileUtils.forceDelete(file);
		}
		boolean testSucceed = file.mkdirs();
		if(!testSucceed){
			throw new IllegalStateException("test dir make failed!" + file);
		}
	}

	
	protected String randomString(){
		
		return randomString(1 << 10);
	}
	
	protected String randomString(int length){
		
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < length ; i++){
			sb.append((char)('a' + (int)(26*Math.random())));
		}
		
		return sb.toString();
		
	}
	
	
	protected String getTestFileDir(){
		String userHome = System.getProperty("user.home");
		String testDir = properties.getProperty("test.file.dir"); 
		return testDir.replace("~", userHome);
		
	} 

	protected void sleepSeconds(int seconds){
		sleep(seconds * 1000);
	}

	protected void sleep(int miliSeconds){
		
		try {
			TimeUnit.MILLISECONDS.sleep(miliSeconds);
		} catch (InterruptedException e) {
		}
	}

	protected String readFileAsString(String fileName) {
		
		return readFileAsString(fileName, Codec.defaultCharset);
	}

	protected String readFileAsString(String fileName, Charset charset) {
		
		FileInputStream fins = null;
		try {
			byte []data = new byte[2048];
			ByteArrayOutputStream baous = new ByteArrayOutputStream();
			fins = new FileInputStream(new File(fileName));
			
			while(true){
				int size = fins.read(data);
				if(size > 0){
					baous.write(data, 0, size);
				}
				if(size == -1){
					break;
				}
			}
			return new String(baous.toByteArray(), charset);
		} catch (FileNotFoundException e) {
			logger.error("[readFileAsString]" + fileName, e);
		} catch (IOException e) {
			logger.error("[readFileAsString]" + fileName, e);
		}finally{
			if(fins != null){
				try {
					fins.close();
				} catch (IOException e) {
					logger.error("[readFileAsString]", e);
				}
			}
		}
		return null;
	}
	
	
	protected String currentTestName(){
		return name.getMethodName();
	} 
	
	@After
	public void afterAbstractTest() throws IOException{

		File file = new File(getTestFileDir());
		FileUtils.forceDelete(file);
		logger.info("[end   test]" + name.getMethodName());
	}
}
