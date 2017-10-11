package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.keeper.container.ComponentRegistryHolder;
import com.ctrip.xpipe.redis.keeper.spring.TestWithoutZkProfile;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@SpringBootApplication
public class AppTest extends AbstractRedisKeeperTest {
	
	@Before
	public void beforeAppTest(){
    	System.setProperty(AbstractProfile.PROFILE_KEY, TestWithoutZkProfile.PROFILE_NO_ZK);
	}
	
	
    @Test
    public void start7080() throws Exception {
        System.setProperty("server.port", "7080");
        System.setProperty(AbstractCoreConfig.KEY_ZK_NAMESPACE, "xpipe_dc1");		
        setReplicationStoreDir();
        start();
    }

    @Test
    public void start7081() throws Exception {
        System.setProperty("server.port", "7081");
        System.setProperty(AbstractCoreConfig.KEY_ZK_NAMESPACE, "xpipe_dc1");
        setReplicationStoreDir();
        start();
    }
    
	private void setReplicationStoreDir() {
        System.setProperty("replication.store.dir", String.format("/opt/data/xpipe%s", System.getProperty("server.port")));
	}


	@Test
    public void start7082() throws Exception {
        System.setProperty("server.port", "7082");
        System.setProperty(AbstractCoreConfig.KEY_ZK_NAMESPACE, "xpipe_dc1");		
        setReplicationStoreDir();
        start();
    }

    @Test
    public void start7180() throws Exception {
        System.setProperty("server.port", "7180");
        System.setProperty(AbstractCoreConfig.KEY_ZK_NAMESPACE, "xpipe_dc2");		
        setReplicationStoreDir();
        start();
    }

    @Test
    public void start7181() throws Exception {
        System.setProperty("server.port", "7181");
        System.setProperty(AbstractCoreConfig.KEY_ZK_NAMESPACE, "xpipe_dc2");		
        setReplicationStoreDir();
        start();
    }
    
    @Test
    public void start7182() throws Exception {
        System.setProperty("server.port", "7182");
        System.setProperty(AbstractCoreConfig.KEY_ZK_NAMESPACE, "xpipe_dc2");		
        setReplicationStoreDir();
        start();
    }


    private void start() throws Exception {
        ConfigurableApplicationContext context =
                new SpringApplicationBuilder(AppTest.class).run();
        initComponentRegistry(context);
        waitForAnyKeyToExit();
    }

    private void initComponentRegistry(ConfigurableApplicationContext  context) throws Exception {
        ComponentRegistry registry = new DefaultRegistry(new CreatedComponentRedistry(),
                new SpringComponentRegistry(context));
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
    }
}
