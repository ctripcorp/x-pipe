package com.ctrip.xpipe.redis.meta.server;


import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskTrigger;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.MetaserverLeaderElector;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import com.ctrip.xpipe.zk.impl.TestZkClient;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.web.context.support.StandardServletEnvironment;

/**
 * @author wenchao.meng
 *
 * Aug 1, 2016
 */
@EnableAutoConfiguration
@Import({com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.class, com.ctrip.xpipe.redis.meta.server.spring.TestProfile.class})
public class TestMetaServer extends AbstractLifecycle{
	
	public static String KEY_CONFIG_FILE = "TEST_META_SERVER_CONFIG_FILE";
	private static final int waitForRestartTimeMills = 1000;
	private static final int zkSessionTimeoutMillis = 5000;
	public static final int total_slots = 16;
	public static final String DEFAULT_CONFIG_FILE = "metaserver--jq.xml";
	private int serverPort;
	private String zkConnectionStr;
	private int serverId; 
	private String configFile = System.getProperty(KEY_CONFIG_FILE, DEFAULT_CONFIG_FILE);
	private ConfigurableApplicationContext context;
	private SpringComponentRegistry manager;
	
	public TestMetaServer(){
		this(1, 9747, 2181);
	}

	public TestMetaServer(int serverId, int serverPort, int zkPort){
		this(serverId, serverPort, String.format("localhost:%d", zkPort), DEFAULT_CONFIG_FILE);
	}
	public TestMetaServer(int serverId, int serverPort, int zkPort, String configFile){
		this(serverId, serverPort, String.format("localhost:%d", zkPort), configFile);
	}

	public TestMetaServer(int serverId, int serverPort, String zkConnectionStr){
		this(serverId, serverPort, zkConnectionStr, DEFAULT_CONFIG_FILE);
	}

	public TestMetaServer(int serverId, int serverPort, String zkConnectionStr, String configFile){
		this.serverId = serverId;
		this.serverPort = serverPort;
		this.zkConnectionStr = zkConnectionStr;
		
	}
	
	
	@Override
	public void doStart() throws Exception{
		
		System.setProperty(DefaultDcMetaCache.MEMORY_META_SERVER_DAO_KEY, configFile);
		System.setProperty("TOTAL_SLOTS", String.valueOf(total_slots));
		
		SpringApplication application = new SpringApplication(TestMetaServer.class);
		application.setBannerMode(Mode.OFF);
		application.setEnvironment(createEnvironment());
		
		context = application.run(new String[]{});
		
		TestZkClient client = context.getBean(TestZkClient.class);
		DefaultZkConfig zkConfig = new DefaultZkConfig(client.getZkAddress());
		zkConfig.setZkSessionTimeoutMillis(zkSessionTimeoutMillis);
		client.setZkConfig(zkConfig);
		client.setZkAddress(zkConnectionStr);

		UnitTestServerConfig config = context.getBean(UnitTestServerConfig.class);
		config.setZkAddress(zkConnectionStr);
		config.setMetaServerId(serverId);
		config.setMetaServerPort(serverPort);
		
		ArrangeTaskTrigger arrangeTaskTrigger = context.getBean(ArrangeTaskTrigger.class);
		arrangeTaskTrigger.setWaitForRestartTimeMills(waitForRestartTimeMills);

		manager = context.getBean(SpringComponentRegistry.class);
		manager.initialize();
		manager.start();
	}
	
	@Override
	public void doStop() throws Exception {
		manager.stop();
		manager.dispose();
		context.close();
	}
	


	public ConfigurableApplicationContext getContext() {
		return context;
	}
	
	
	private ConfigurableEnvironment createEnvironment() {
		
		return new MyEnvironment();
	}
	
	class MyEnvironment extends StandardServletEnvironment{
		
		@Override
		protected void customizePropertySources(MutablePropertySources propertySources) {
			super.customizePropertySources(propertySources);
			propertySources.addFirst(new PropertySource<Object>("TestAppServerProperty"){

				@Override
				public Object getProperty(String name) {
					
					if(name.equals("server.port")){
						return String.valueOf(serverPort);
					}
					return null;
				}
				
			});
		}
	}
	
	public boolean isLeader(){

		if(getLifecycleState().isStarted()){
			try{
				MetaserverLeaderElector metaserverLeaderElector = context.getBean(MetaserverLeaderElector.class);
				return metaserverLeaderElector.amILeader();
			}catch(Exception e){
				return false;
			}
		}
		return false;
	}
	
	public int getServerId() {
		return serverId;
	}
	
	public int getServerPort() {
		return serverPort;
	}
	
	public String getAddress(){
		
		return String.format("http://127.0.0.1:%d", getServerPort());
	}
	
	public static int getWaitforrestarttimemills() {
		return waitForRestartTimeMills;
	}
	
	public static int getZksessiontimeoutmillis() {
		return zkSessionTimeoutMillis;
	}
	
	@Override
	public String toString() {
		return String.format("serverId:%d, serverPort:%d", serverId, serverPort);
	}
	
	public static void main(String []argc) throws Exception{
		
		TestMetaServer server = new TestMetaServer(1, 9747, 2181);
		server.initialize();
		server.start();		
//		new TestAppServer(2, 9748, 2182).start();
		
	}

}
