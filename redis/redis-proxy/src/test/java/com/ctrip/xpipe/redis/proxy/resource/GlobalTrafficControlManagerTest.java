package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test for GlobalTrafficControlManager
 * 
 * @author system
 */
public class GlobalTrafficControlManagerTest {

    private GlobalTrafficControlManager manager;
    private TestProxyConfig config;
    private EventLoopGroup eventLoopGroup;
    private ScheduledExecutorService scheduledExecutor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        
        config = new TestProxyConfig();
        eventLoopGroup = new NioEventLoopGroup(1);
        scheduledExecutor = new ScheduledThreadPoolExecutor(1);
        
        manager = new GlobalTrafficControlManager(config, eventLoopGroup, scheduledExecutor);
    }

    @After
    public void tearDown() {
        if (manager != null) {
            manager.release();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
    }

    @Test
    public void testTrafficControlDisabledByDefault() {
        assertFalse("Traffic control should be disabled by default", manager.isTrafficControlEnabled());
        assertNull("Traffic shaping handler should be null when disabled", manager.getTrafficShapingHandler());
    }

    @Test
    public void testTrafficControlEnabled() {
        // Create a config with traffic control enabled
        TestProxyConfig enabledConfig = new TestProxyConfig() {
            @Override
            public boolean isCrossRegionTrafficControlEnabled() {
                return true;
            }
            
            @Override
            public long getCrossRegionTrafficControlLimit() {
                return 1048576L; // 1MB/s
            }
        };
        
        GlobalTrafficControlManager newManager = new GlobalTrafficControlManager(enabledConfig, eventLoopGroup, scheduledExecutor);
        
        try {
            assertTrue("Traffic control should be enabled", newManager.isTrafficControlEnabled());
            assertNotNull("Traffic shaping handler should not be null when enabled", newManager.getTrafficShapingHandler());
        } finally {
            newManager.release();
        }
    }

    @Test
    public void testConfigChangeListener() {
        // Create a config with traffic control enabled
        TestProxyConfig enabledConfig = new TestProxyConfig() {
            @Override
            public boolean isCrossRegionTrafficControlEnabled() {
                return true;
            }
            
            @Override
            public long getCrossRegionTrafficControlLimit() {
                return 1048576L; // 1MB/s
            }
        };
        
        GlobalTrafficControlManager newManager = new GlobalTrafficControlManager(enabledConfig, eventLoopGroup, scheduledExecutor);
        
        try {
            assertTrue("Traffic control should be enabled", newManager.isTrafficControlEnabled());
            
            // Trigger config change
            newManager.onConfigChanged();
            
            // Verify the change took effect
            assertTrue("Traffic control should still be enabled after config change", newManager.isTrafficControlEnabled());
            
        } finally {
            newManager.release();
        }
    }

    @Test
    public void testRelease() {
        // Create a config with traffic control enabled
        TestProxyConfig enabledConfig = new TestProxyConfig() {
            @Override
            public boolean isCrossRegionTrafficControlEnabled() {
                return true;
            }
            
            @Override
            public long getCrossRegionTrafficControlLimit() {
                return 1048576L; // 1MB/s
            }
        };
        
        GlobalTrafficControlManager newManager = new GlobalTrafficControlManager(enabledConfig, eventLoopGroup, scheduledExecutor);
        
        assertTrue("Traffic control should be enabled", newManager.isTrafficControlEnabled());
        
        newManager.release();
        
        assertFalse("Traffic control should be disabled after release", newManager.isTrafficControlEnabled());
        assertNull("Traffic shaping handler should be null after release", newManager.getTrafficShapingHandler());
    }
} 