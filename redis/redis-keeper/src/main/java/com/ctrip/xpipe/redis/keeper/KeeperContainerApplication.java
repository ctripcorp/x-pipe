package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.keeper.container.ComponentRegistryHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@SpringBootApplication
public class KeeperContainerApplication {
	
	private static Logger logger = LoggerFactory.getLogger(KeeperContainerApplication.class);
	
    public static void main(String[] args) throws Exception {
    	
        SpringApplication application = new SpringApplication(KeeperContainerApplication.class);
        application.setRegisterShutdownHook(false);
        final ConfigurableApplicationContext context = application.run(args);
        
        final ComponentRegistry registry = initComponentRegistry(context);
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				try {
					logger.info("[run][shutdown][stop]");
					registry.stop();
				} catch (Exception e) {
					logger.error("[run][shutdown][stop]", e);
				}
				try {
					logger.info("[run][shutdown][dispose]");
					registry.dispose();
				} catch (Exception e) {
					logger.error("[run][shutdown][dispose]", e);
				}
				
				try {
					logger.info("[run][shutdown][destroy]");
					registry.destroy();
				} catch (Exception e) {
					logger.error("[run][shutdown][destroy]", e);
				}
			}
		}));

    }

    private static ComponentRegistry initComponentRegistry(ConfigurableApplicationContext context) throws Exception {
    	
        final ComponentRegistry registry = new DefaultRegistry(new CreatedComponentRedistry(),
                new SpringComponentRegistry(context));
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
        return registry;
    }
}
