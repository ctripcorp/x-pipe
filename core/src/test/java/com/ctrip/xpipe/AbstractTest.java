package com.ctrip.xpipe;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.slf4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.exception.DefaultExceptionHandler;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.zk.ZkTestServer;

/**
 * @author wenchao.meng
 *
 * 2016年3月28日 下午5:44:47
 */
public class AbstractTest {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	protected ExecutorService executors = Executors.newCachedThreadPool();

	protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(OsUtils.getCpuCount());
	
	private ComponentRegistry componentRegistry;

	@Rule
	public TestName name = new TestName();
	
	private static Properties properties = new Properties();
	
	private ComponentRegistry startedComponentRegistry;
	
	@Before
	public void beforeAbstractTest() throws Exception{
		
		logger.info(remarkableMessage("[begin test][{}]{}") , getClass().getSimpleName(), name.getMethodName());
		
		System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
		System.setProperty("cat.client.enabled", "false");

		setProperties();
		componentRegistry = new DefaultRegistry(new CreatedComponentRedistry(), getSpringRegistry());

		startedComponentRegistry = new CreatedComponentRedistry();
		startedComponentRegistry.initialize();
		startedComponentRegistry.start();
		
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
		if(file.exists() && deleteTestDir()){
			FileUtils.forceDelete(file);
		}
		
		if(!file.exists()){
			boolean testSucceed = file.mkdirs();
			if(!testSucceed){
				throw new IllegalStateException("test dir make failed!" + file);
			}
		}
	}
	
	protected String getTestName(){
		return name.getMethodName();
	}

	protected void setProperties() {
		
	}

	protected boolean deleteTestDir() {
		return true;
	}

	private ComponentRegistry getSpringRegistry() {
		
		ApplicationContext applicationContext = createSpringContext();
		if(applicationContext != null){
			return new SpringComponentRegistry(applicationContext);
		}
		return null;
	}

	/**
	 * to be overriden by subclasses
	 * @return
	 */
	protected  ApplicationContext createSpringContext() {
		return null;
	}
	
	protected<T> T getBean(Class<T> clazz){
		
		return getRegistry().getComponent(clazz);
	}

	protected void initRegistry() throws Exception{
		
		componentRegistry.initialize();
	}

	protected void startRegistry() throws Exception{
		
		componentRegistry.start();
	}

	public static String randomString(){
		
		return randomString(1 << 10);
	}
	
	public static String randomString(int length){
		
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < length ; i++){
			sb.append((char)('a' + (int)(26*Math.random())));
		}
		
		return sb.toString();
		
	}
	
	
	protected String getTestFileDir(){
		
		String userHome = getUserHome();
		String testDir = properties.getProperty("test.file.dir"); 
		String result = testDir.replace("~", userHome);
		return result + "/" + currentTestName();
	}
	
	public static String getUserHome(){
		
		return System.getProperty("user.home");
	}

	
	protected void sleepSeconds(int seconds){
		sleep(seconds * 1000);
	}

	protected void sleepIgnoreInterrupt(int time) {
		long future = System.currentTimeMillis() + time;

		while(true){
			long left = future - System.currentTimeMillis();
			if(left <= 0){
				break;
			}
			if(left > 0){
				try {
					TimeUnit.MILLISECONDS.sleep(left);
				} catch (InterruptedException e) {
				}
			}
		}
		
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
	
	protected void add(Object lifecycle) throws Exception{
		this.componentRegistry.add(lifecycle);
	}

	protected void remove(Object lifecycle) throws Exception{
		this.componentRegistry.remove(lifecycle);
	}
	
	public ComponentRegistry getRegistry() {
		return componentRegistry;
	}

	
	protected String currentTestName(){
		return name.getMethodName();
	} 
	
	public static int portUsable(int fromPort){
		
		for(int i=fromPort;i<fromPort + 100;i++){
			if(isUsable(i)){
				return i;
			}
		}
		
		throw new IllegalStateException("unfonud usable port from %d" + fromPort);
	}

	
	public static int randomPort(){
		return randomPort(10000, 20000); 
	}
	
	
	/**
	 * find an available port from min to max
	 * @param min
	 * @param max
	 * @return
	 */
	public static int randomPort(int min, int max) {

		int i = min;
		for(;i<=max;i++){
			if(isUsable(i)){
				return i;
			}
		}
		
		throw new IllegalStateException(String.format("random port not found:(%d, %d)", min , max));
	}
	
	private static boolean isUsable(int port) {
		
		try(ServerSocket s = new ServerSocket()){
			s.bind(new InetSocketAddress(port));
			return true;
		} catch (IOException e) {
		}
		return false;
	}

	protected static int randomInt(int begin, int end){
		return (int) (begin + Math.random() * (end - begin));
	}

	protected Integer randomInt() {
		
		return (int)(Math.random() * Integer.MAX_VALUE);
	}

	protected String remarkableMessage(String msg) {
		return String.format("\r\n--------------------------------------------------%s--------------------------------------------------\r\n", msg);
	}

	protected void waitForAnyKeyToExit() throws IOException{
		System.out.println("type any key to exit..................");
		System.in.read();
	}

	protected ZkTestServer startRandomZk() {
		
		int zkPort = randomInt(2181, 2281);
		return startZk(zkPort);
	}

	protected ZkTestServer startZk(int zkPort) {
		try {
			logger.info(remarkableMessage("[startZK]{}"), zkPort);
			ZkTestServer zkTestServer = new ZkTestServer(zkPort);
			zkTestServer.initialize();
			zkTestServer.start();
			add(zkTestServer);
			return zkTestServer;
		} catch (Exception e) {
			logger.error("[startZk]", e);
			throw new IllegalStateException("[startZk]" + zkPort, e);
		}
	}
	
	
	
	
	public static int defaultZkPort(){
		return 2181;
	}

	public static int defaultMetaServerPort(){
		return 9747;
	}

	protected Server startEchoServer() throws Exception {
		return startServer(new IoActionFactory() {
			
			@Override
			public IoAction createIoAction() {
				return new AbstractIoAction(){
					
					private String line;

					@Override
					protected Object doRead(InputStream ins) throws IOException {
						line = readLine(ins);
						logger.info("[doRead]{}", line);
						return line;
					}

					@Override
					protected void doWrite(OutputStream ous) throws IOException {
						
						String []sp = line.split("\\s+");
						if(sp.length >= 1){
							if(sp[0].equalsIgnoreCase("sleep")){
								int sleep = Integer.parseInt(sp[1]);
								logger.info("[sleep]{}", sleep);
								sleepIgnoreInterrupt(sleep);
							}
						}
						logger.info("[doWrite]{}", line);
						ous.write(line.getBytes());
						ous.flush();
					}
				};
			}
		});
	}

	protected Server startServer(IoActionFactory ioActionFactory) throws Exception{
		
		int serverPort = randomPort();
		Server server = new Server(serverPort, ioActionFactory);
		server.initialize();
		server.start();
		add(server);
		return server;
	}
	
	protected Server startServer(final String result) throws Exception {
		
		IoActionFactory ioActionFactory = new IoActionFactory() {
			
			@Override
			public IoAction createIoAction() {
				return new AbstractIoAction() {
					
					@Override
					protected void doWrite(OutputStream ous) throws IOException {
						ous.write(result.getBytes());
					}
					
					@Override
					protected Object doRead(InputStream ins) throws IOException {
						String line = readLine(ins);
						logger.info("[doRead]{}", line);
						return line;
					}
				};
			}
		};
		return startServer(ioActionFactory);
	}


	@After
	public void afterAbstractTest() throws IOException{
		
		try {
			LifecycleHelper.stopIfPossible(componentRegistry);
			LifecycleHelper.disposeIfPossible(componentRegistry);
			componentRegistry.destroy();
		} catch (Exception e) {
			logger.error("[afterAbstractTest]", e);
		}
		File file = new File(getTestFileDir());
		FileUtils.deleteQuietly(file);
		logger.info(remarkableMessage("[end   test][{}]{}"), getClass().getSimpleName(), name.getMethodName());
	}
}
