package com.ctrip.xpipe.redis.meta.server.cluster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.web.context.support.StandardServletEnvironment;

import com.ctrip.xpipe.zk.ZkTestServer;

/**
 * @author wenchao.meng
 *
 * Aug 1, 2016
 */
@EnableAutoConfiguration
@Import(com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.class)
public class TestAppServer {
	
	private int serverPort;
	private int zkPort;
	
	public TestAppServer(int serverPort, int zkPort){
		this.serverPort = serverPort;
		this.zkPort = zkPort;
	}
	
	
	public void start() throws Exception{
		
		startZk();
		SpringApplication application = new SpringApplication(TestAppServer.class);
		application.setEnvironment(createEnvironment());
		application.run(new String[]{});
	}

	private void startZk() throws Exception {
		ZkTestServer server = new ZkTestServer(zkPort);
		server.initialize();
		server.start();
	}


	private ConfigurableEnvironment createEnvironment() {
		
		return null;
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
	
	public static void main(String []argc) throws Exception{
		
		new TestAppServer(9747, 2181).start();
//		new TestAppServer(9748, 2182).start();
		
	}
}
