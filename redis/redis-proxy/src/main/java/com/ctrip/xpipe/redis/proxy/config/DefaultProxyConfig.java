package com.ctrip.xpipe.redis.proxy.config;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.redis.proxy.spring.Production;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */

@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyConfig implements ProxyConfig {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyConfig.class);

    private Config config;

    private static final String PROXY_PROPERTIES_PATH = String.format("/opt/data/%s", FoundationService.DEFAULT.getAppId());

    private static final String PROXY_PROPERTIES_FILE = "xpipe.properties";

    private static final String KEY_ENDPOINT_HEALTH_CHECK_INTERVAL = "proxy.endpoint.check.interval.sec";

    private static final String KEY_TRAFFIC_REPORT_INTERVAL = "proxy.traffic.report.interval.milli";

    private static final String KEY_FRONTEND_TCP_PORT = "proxy.frontend.tcp.port";

    private static final String KEY_FRONTEND_TLS_PORT = "proxy.frontend.tls.port";

    private static final String KEY_NO_TLS_NETTY_HANDLER = "proxy.no.tls.netty.handler";

    private static final String KEY_INTERNAL_NETWORK_PREFIX = "proxy.internal.network.prefix";

    private static final String KEY_RECV_BUFFER_SIZE = "proxy.recv.buffer.size";

    private static final String KEY_MAX_PACKET_BUFFER_SIZE = "proxy.max.packet.buffer.size";

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("DefaultProxyConfig"));

    public DefaultProxyConfig() {
        config = initConfig();
        scheduledFresh();
    }

    public void scheduledFresh() {
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                config = initConfig();
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    private Config initConfig() {
        CompositeConfig compositeConfig = new CompositeConfig();
        try {
            compositeConfig.addConfig(new DefaultFileConfig(PROXY_PROPERTIES_PATH, PROXY_PROPERTIES_FILE));
        } catch (Exception e) {
            logger.info("[DefaultProxyConfig]{}", e);
        }

        try {
            compositeConfig.addConfig(new DefaultFileConfig());
        } catch (Exception e) {
            logger.info("[DefaultProxyConfig]{}", e);
        }
        return compositeConfig;
    }

    @Override
    public int frontendTcpPort() {
        return getIntProperty(KEY_FRONTEND_TCP_PORT, 80);
    }

    @Override
    public int frontendTlsPort() {
        return getIntProperty(KEY_FRONTEND_TLS_PORT, 443);
    }

    @Override
    public long getTrafficReportIntervalMillis() {
        return getLongProperty(KEY_TRAFFIC_REPORT_INTERVAL, 30000L);
    }

    @Override
    public int endpointHealthCheckIntervalSec() {
        return getIntProperty(KEY_ENDPOINT_HEALTH_CHECK_INTERVAL, 60);
    }

    @Override
    public boolean noTlsNettyHandler() {
        return getBooleanProperty(KEY_NO_TLS_NETTY_HANDLER, false);
    }

    @Override
    public int getFixedRecvBufferSize() {
        return getIntProperty(KEY_RECV_BUFFER_SIZE, 1024);
    }

    @Override
    public String[] getInternalNetworkPrefix() {
        String internalNetworkPrefix = getProperty(KEY_INTERNAL_NETWORK_PREFIX, "10");
        return IpUtils.splitIpAddr(internalNetworkPrefix);
    }


    @Override
    public String getPassword() {
        return getProperty(KEY_CERT_PASSWORD, FoundationService.DEFAULT.getAppId());
    }

    @Override
    public String getServerCertFilePath() {
        return getProperty(KEY_SERVER_CERT_FILE_PATH, "/opt/data/100013684/xpipe-server.jks");
    }

    @Override
    public String getClientCertFilePath() {
        return getProperty(KEY_CLIENT_CERT_FILE_PATH, "/opt/data/100013684/xpipe-client.jks");
    }

    @Override
    public String getCertFileType() {
        return getProperty(KEY_CERT_FILE_TYPE, "JKS");
    }

    @Override
    public int getMaxPacketBufferSize() {
        return getIntProperty(KEY_MAX_PACKET_BUFFER_SIZE, 2048);
    }

    protected String getProperty(String key, String defaultValue){
        return config.get(key, defaultValue);
    }

    protected Integer getIntProperty(String key, Integer defaultValue){

        String value = config.get(key);
        if(value == null){
            return defaultValue;
        }
        return Integer.parseInt(value.trim());

    }

    protected Long getLongProperty(String key, Long defaultValue){

        String value = config.get(key);
        if(value == null){
            return defaultValue;
        }
        //TODO cache value to avoid convert each time
        return Long.parseLong(value.trim());

    }

    protected Boolean getBooleanProperty(String key, Boolean defaultValue){

        String value = config.get(key);
        if(value == null){
            return defaultValue;
        }

        return Boolean.parseBoolean(value.trim());
    }
}
