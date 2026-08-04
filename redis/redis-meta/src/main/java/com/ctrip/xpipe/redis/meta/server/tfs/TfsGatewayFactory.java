package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * Creates {@link TfsGateway} from QConfig host.
 * Host/appId 热更新（D23）：选型时读最新 host；{@link HttpTfsGateway} 持 Config，每次调用读最新 host/appId。
 */
public final class TfsGatewayFactory {

    public static final String MOCK_GATEWAY_PREFIX = "mock://";

    private static final TfsGateway MOCK_GATEWAY = new MockTfsGateway();

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
        return new HttpTfsGateway(config);
    }

    /**
     * Host-only 选型（单测便捷）；Real 无 Config 时 appId 固定为 0。
     */
    public static TfsGateway create(String host) {
        if (isMockHost(host)) {
            return MOCK_GATEWAY;
        }
        return new HttpTfsGateway(host, 0L);
    }
}
