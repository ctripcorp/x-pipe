package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.redis.proxy.config.DefaultProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Global traffic control manager for cross-region proxy tunnels
 * Manages a single GlobalTrafficShapingHandler instance to control overall throughput
 * 
 * @author system
 */
public class GlobalTrafficControlManager implements DefaultProxyConfig.ConfigChangeListener {

    private static final Logger logger = LoggerFactory.getLogger(GlobalTrafficControlManager.class);

    private final ProxyConfig config;
    private final ScheduledExecutorService scheduledExecutor;
    
    private final AtomicReference<GlobalTrafficShapingHandler> trafficShapingHandlerRef = new AtomicReference<>();

    public GlobalTrafficControlManager(ProxyConfig config, ScheduledExecutorService scheduledExecutor) {
        this.config = config;
        this.scheduledExecutor = scheduledExecutor;
        initializeTrafficShapingHandler();
        
        // Register as config change listener if config supports it
        if (config instanceof DefaultProxyConfig) {
            ((DefaultProxyConfig) config).addConfigChangeListener(this);
        }
    }

    private void initializeTrafficShapingHandler() {
        logger.info("[initializeTrafficShapingHandler]");
        GlobalTrafficShapingHandler handler = new GlobalTrafficShapingHandler(scheduledExecutor, 0, 0, 1000L, 1000L);
        trafficShapingHandlerRef.set(handler);
        updateTrafficControlSettings();
    }

    /**
     * Update traffic control settings based on current configuration
     */
    public void updateTrafficControlSettings() {
        if (config.isCrossRegionTrafficControlEnabled()) {
            long limit = Math.max(0, config.getCrossRegionTrafficControlLimit());
            GlobalTrafficShapingHandler currentHandler = trafficShapingHandlerRef.get();
            if (currentHandler.getReadLimit() != limit || currentHandler.getWriteLimit() != limit) {
                currentHandler.configure(limit, limit);
                logger.info("[updateTrafficControlSettings] Updated traffic shaping handler with new limit: {} bytes/s", limit);
            }
        } else {
            // Disable traffic control
            GlobalTrafficShapingHandler currentHandler = trafficShapingHandlerRef.get();
            if (currentHandler.getReadLimit() != 0 || currentHandler.getWriteLimit() != 0) {
                currentHandler.configure(0, 0);
                logger.info("[updateTrafficControlSettings] Disabled traffic control");
            }
        }
    }

    /**
     * Get the current traffic shaping handler if traffic control is enabled
     * @return GlobalTrafficShapingHandler or null if disabled
     */
    public GlobalTrafficShapingHandler getTrafficShapingHandler() {
        return trafficShapingHandlerRef.get();
    }

    /**
     * Check if traffic control is currently enabled
     * @return true if enabled, false otherwise
     */
    public boolean isTrafficControlEnabled() {
        return config.isCrossRegionTrafficControlEnabled() && trafficShapingHandlerRef.get() != null;
    }

    /**
     * Release resources
     */
    public void release() {
        GlobalTrafficShapingHandler handler = trafficShapingHandlerRef.getAndSet(null);
        if (handler != null) {
            handler.release();
            logger.info("[release] Released traffic shaping handler");
        }
        
        // Unregister from config change listeners
        if (config instanceof DefaultProxyConfig) {
            ((DefaultProxyConfig) config).removeConfigChangeListener(this);
        }
    }

    @Override
    public void onConfigChanged() {
        logger.info("[onConfigChanged] Configuration changed, updating traffic control settings");
        updateTrafficControlSettings();
    }
} 