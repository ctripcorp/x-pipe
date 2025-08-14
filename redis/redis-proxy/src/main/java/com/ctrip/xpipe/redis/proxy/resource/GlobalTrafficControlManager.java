package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.redis.proxy.config.DefaultProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import io.netty.channel.EventLoopGroup;
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
    private final EventLoopGroup eventLoopGroup;
    private final ScheduledExecutorService scheduledExecutor;
    
    private final AtomicReference<GlobalTrafficShapingHandler> trafficShapingHandlerRef = new AtomicReference<>();

    public GlobalTrafficControlManager(ProxyConfig config, EventLoopGroup eventLoopGroup, ScheduledExecutorService scheduledExecutor) {
        this.config = config;
        this.eventLoopGroup = eventLoopGroup;
        this.scheduledExecutor = scheduledExecutor;
        initializeTrafficShapingHandler();
        
        // Register as config change listener if config supports it
        if (config instanceof DefaultProxyConfig) {
            ((DefaultProxyConfig) config).addConfigChangeListener(this);
        }
    }

    private void initializeTrafficShapingHandler() {
        if (config.isCrossRegionTrafficControlEnabled()) {
            long limit = config.getCrossRegionTrafficControlLimit();
            GlobalTrafficShapingHandler handler = new GlobalTrafficShapingHandler(scheduledExecutor, limit, limit, 1000L, 1000L);
            trafficShapingHandlerRef.set(handler);
            logger.info("[initializeTrafficShapingHandler] Global traffic control enabled with limit: {} bytes/s", limit);
        } else {
            logger.info("[initializeTrafficShapingHandler] Global traffic control disabled");
        }
    }

    /**
     * Update traffic control settings based on current configuration
     */
    public void updateTrafficControlSettings() {
        if (config.isCrossRegionTrafficControlEnabled()) {
            long limit = config.getCrossRegionTrafficControlLimit();
            GlobalTrafficShapingHandler currentHandler = trafficShapingHandlerRef.get();
            
            if (currentHandler == null) {
                // Create new handler if not exists
                GlobalTrafficShapingHandler newHandler = new GlobalTrafficShapingHandler(scheduledExecutor, limit, limit, 1000L, 1000L);
                trafficShapingHandlerRef.set(newHandler);
                logger.info("[updateTrafficControlSettings] Created new traffic shaping handler with limit: {} bytes/s", limit);
            } else {
                // Update existing handler - recreate it since configure method signature is different
                currentHandler.release();
                GlobalTrafficShapingHandler newHandler = new GlobalTrafficShapingHandler(scheduledExecutor, limit, limit, 1000L, 1000L);
                trafficShapingHandlerRef.set(newHandler);
                logger.info("[updateTrafficControlSettings] Updated traffic shaping handler with new limit: {} bytes/s", limit);
            }
        } else {
            // Disable traffic control
            GlobalTrafficShapingHandler currentHandler = trafficShapingHandlerRef.getAndSet(null);
            if (currentHandler != null) {
                currentHandler.release();
                logger.info("[updateTrafficControlSettings] Disabled traffic control");
            }
        }
    }

    /**
     * Get the current traffic shaping handler if traffic control is enabled
     * @return GlobalTrafficShapingHandler or null if disabled
     */
    public GlobalTrafficShapingHandler getTrafficShapingHandler() {
        if (config.isCrossRegionTrafficControlEnabled()) {
            return trafficShapingHandlerRef.get();
        }
        return null;
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