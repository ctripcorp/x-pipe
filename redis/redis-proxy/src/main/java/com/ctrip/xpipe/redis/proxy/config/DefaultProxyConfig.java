package com.ctrip.xpipe.redis.proxy.config;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.redis.proxy.spring.Production;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyConfig extends AbstractConfigBean implements ProxyConfig {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyConfig.class);

    private static final String PROXY_PROPERTIES_PATH = String.format("/opt/data/%s", FoundationService.DEFAULT.getAppId());

    private static final String PROXY_PROPERTIES_FILE = "xpipe.properties";

    private static final String KEY_ENDPOINT_HEALTH_CHECK_INTERVAL = "proxy.endpoint.check.interval.sec";

    private static final String KEY_TRAFFIC_REPORT_INTERVAL = "proxy.traffic.report.interval.milli";

    private static final String KEY_FRONTEND_TCP_PORT = "proxy.frontend.tcp.port";

    private static final String KEY_FRONTEND_TLS_PORT = "proxy.frontend.tls.port";

    private static final String KEY_DEBUG_TUNNEL = "proxy.debug.tunnel";

    private static final String KEY_NOT_INTEREST_IP = "proxy.not.interest.ips";

    private static final String KEY_NO_TLS_NETTY_HANDLER = "proxy.no.tls.netty.handler";

    private String[] notInterests;

    @Resource(name = Production.GLOBAL_SCHEDULED)
    private ScheduledExecutorService scheduled;

    public DefaultProxyConfig() {
        setConfig(initConfig());
    }

    @PostConstruct
    public void scheduledFresh() {
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                setConfig(initConfig());
                refreshNotInterest();
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
    public boolean debugTunnel() {
        return getBooleanProperty(KEY_DEBUG_TUNNEL, false);
    }

    @Override
    public boolean notInterest(InetSocketAddress address) {
        if(notInterests == null || notInterests.length == 0) {
            refreshNotInterest();
        }
        if(notInterests == null) {
            return false;
        }
        for(String ip : notInterests) {
            if(address.getHostName().equals(ip))
                return true;
        }
        return false;
    }

    @Override
    public boolean noTlsNettyHandler() {
        return getBooleanProperty(KEY_NO_TLS_NETTY_HANDLER, false);
    }

    private void refreshNotInterest() {
        String ips = getProperty(KEY_NOT_INTEREST_IP, "0.0.0.0");
        notInterests = StringUtil.splitRemoveEmpty("\\s*,\\s*", ips);
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
}
