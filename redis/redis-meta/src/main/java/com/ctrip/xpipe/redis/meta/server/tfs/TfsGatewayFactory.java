package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Creates {@link TfsGateway} from QConfig host.
 * Host/appId 热更新（D23）：选型时读最新 host；Real 占位持有 Config，Phase 10 HTTP 实现同样每次读 Config。
 */
public final class TfsGatewayFactory {

    public static final String MOCK_GATEWAY_PREFIX = "mock://";

    private static final Logger logger = LoggerFactory.getLogger(TfsGatewayFactory.class);

    private static final TfsGateway MOCK_GATEWAY = new MockTfsGateway();

    private static final AtomicBoolean UNIMPLEMENTED_LOGGED = new AtomicBoolean(false);

    private TfsGatewayFactory() {
    }

    public static boolean isMockHost(String host) {
        return StringUtil.isEmpty(host) || host.startsWith(MOCK_GATEWAY_PREFIX);
    }

    public static TfsGateway create(MetaServerConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("metaServerConfig required");
        }
        if (isMockHost(config.getTfsGatewayHost())) {
            return MOCK_GATEWAY;
        }
        if (UNIMPLEMENTED_LOGGED.compareAndSet(false, true)) {
            logger.error("[create][real TFS gateway HTTP not implemented yet (Phase 10), placeholder]host={}, appId={}",
                    config.getTfsGatewayHost(), config.getTfsGatewayAppId());
        }
        return new UnimplementedTfsGateway(config);
    }

    /**
     * Host-only 选型（单测便捷）；Real 占位无 Config，无法读热更新 appId。
     */
    public static TfsGateway create(String host) {
        if (isMockHost(host)) {
            return MOCK_GATEWAY;
        }
        if (UNIMPLEMENTED_LOGGED.compareAndSet(false, true)) {
            logger.error("[create][real TFS gateway HTTP not implemented yet (Phase 10), placeholder]host={}", host);
        }
        return new UnimplementedTfsGateway(host);
    }

    /**
     * Phase 9 占位；Phase 10 由 {@code HttpTfsGateway} 替换。持有 Config 以便读最新 host/appId。
     */
    static final class UnimplementedTfsGateway implements TfsGateway {

        private final MetaServerConfig config;
        private final String fixedHost;

        private UnimplementedTfsGateway(MetaServerConfig config) {
            this.config = config;
            this.fixedHost = null;
        }

        private UnimplementedTfsGateway(String host) {
            this.config = null;
            this.fixedHost = host;
        }

        @Override
        public void forceCloseDir(String fsId, String dirPath, String podIp) {
            String host = config != null ? config.getTfsGatewayHost() : fixedHost;
            long appId = config != null ? config.getTfsGatewayAppId() : -1L;
            throw new UnsupportedOperationException(
                    "Real TFS gateway is not implemented yet (Phase 10): host=" + host + ", appId=" + appId);
        }

        MetaServerConfig getConfig() {
            return config;
        }
    }
}
