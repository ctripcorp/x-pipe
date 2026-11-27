package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
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
    
	/**
	 * Local layout of the keeper-ops E2E data (KeeperOpsE2EConsoleTest), one process per container:
	 * <pre>
	 * 7080 / 7081  jq TFS  (BU 44)  shared store  ~/rsd/tfs-jq
	 * 7180 / 7181  oy TFS  (BU 45)  shared store  ~/rsd/tfs-oy
	 * 7082 / 7083 / 7182 / 7183  BM (NORMAL mode)  private store ~/rsd/bm-&lt;port&gt;
	 * </pre>
	 * Each process also gets its own hostname ({@code <host>-<port>}), so their disk probes
	 * ({@code <store>/disk_check/<hostname>}) never share a file. Other ports keep the old defaults.
	 */
	private void setReplicationStoreDir() {
        String port = System.getProperty("server.port");
        System.setProperty("replication.store.dir", String.format("/opt/data/xpipe%s", port));

        String root = getUserHome() + "/rsd";
        String storeDir = null;
        String mode = null;
        switch (port) {
            case "7080": case "7081":
                storeDir = root + "/tfs-jq"; mode = KeeperContainerConfig.MODE_TFS; break;
            case "7180": case "7181":
                storeDir = root + "/tfs-oy"; mode = KeeperContainerConfig.MODE_TFS; break;
            case "7082": case "7083": case "7182": case "7183":
                storeDir = root + "/bm-" + port; mode = KeeperContainerConfig.MODE_NORMAL; break;
            default:
        }
        if (storeDir != null) {
            System.setProperty(TestWithoutZkProfile.KEY_CONTAINER_STORE_DIR, storeDir);
            System.setProperty(TestWithoutZkProfile.KEY_CONTAINER_MODE, mode);
        }
        System.setProperty(LocalContainerFoundationService.KEY_HOSTNAME,
                new DefaultFoundationService().getHostName() + "-" + port);
	}


	@Test
    public void start7082() throws Exception {
        System.setProperty("server.port", "7082");
        System.setProperty(AbstractCoreConfig.KEY_ZK_NAMESPACE, "xpipe_dc1");		
        setReplicationStoreDir();
        start();
    }

    @Test
    public void start7083() throws Exception {
        System.setProperty("server.port", "7083");
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

    @Test
    public void start7183() throws Exception {
        System.setProperty("server.port", "7183");
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
