package com.ctrip.xpipe.redis.meta.server.cluster;




import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.web.context.support.StandardServletEnvironment;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskTrigger;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.MetaserverLeaderElector;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;

/**
 * @author wenchao.meng
 *
 * Aug 1, 2016
 */
@EnableAutoConfiguration
@Import(com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.class)
public class TestAppServer extends AbstractLifecycle{
	
	private static final int waitForRestartTimeMills = 1000;
	private static final int zkSessionTimeoutMillis = 5000;
	public static final int total_slots = 16;
	private int serverPort;
	private int zkPort;
	private int serverId; 
	private String configFile = "meta-test.xml";
	private ConfigurableApplicationContext context;
	
	public TestAppServer(){
		
	}

	public TestAppServer(int serverId, int serverPort, int zkPort){
		this(serverId, serverPort, zkPort, "meta-test.xml");
	}
	

	public TestAppServer(int serverId, int serverPort, int zkPort, String configFile){
		this.serverId = serverId;
		this.serverPort = serverPort;
		this.zkPort = zkPort;
		
	}
	
	
	@Override
	public void doStart() throws Exception{
		
		System.setProperty(DefaultDcMetaCache.MEMORY_META_SERVER_DAO_KEY, configFile);
		System.setProperty("TOTAL_SLOTS", String.valueOf(total_slots));
		System.setProperty(SpringComponentLifecycleManager.SPRING_COMPONENT_START_KEY, "false");
		
		SpringApplication application = new SpringApplication(TestAppServer.class);
		application.setEnvironment(createEnvironment());
		context = application.run(new String[]{});
		
		DefaultZkClient client = context.getBean(DefaultZkClient.class);
		DefaultZkConfig zkConfig = new DefaultZkConfig();
		zkConfig.setZkSessionTimeoutMillis(zkSessionTimeoutMillis);
		client.setZkConfig(zkConfig);
		client.setZkAddress(getZkAddress());

		DefaultMetaServerConfig config = context.getBean(DefaultMetaServerConfig.class);
		config.setZkConnectionString(getZkAddress());
		config.setDefaultMetaServerId(serverId);
		config.setDefaultServerPort(serverPort);
		
		ArrangeTaskTrigger arrangeTaskTrigger = context.getBean(ArrangeTaskTrigger.class);
		arrangeTaskTrigger.setWaitForRestartTimeMills(waitForRestartTimeMills);

		SpringComponentLifecycleManager manager = context.getBean(SpringComponentLifecycleManager.class);
		manager.startAll();
	}
	
	@Override
	public void doStop() throws Exception {
		context.close();
	}
	


	private String getZkAddress() {
		return String.format("localhost:%d", zkPort);
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
	
	public int getZkPort() {
		return zkPort;
	}
	
	public int getServerPort() {
		return serverPort;
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
		
		new TestAppServer(1, 9747, 2181).start();
		new TestAppServer(2, 9748, 2182).start();
		
	}
}
