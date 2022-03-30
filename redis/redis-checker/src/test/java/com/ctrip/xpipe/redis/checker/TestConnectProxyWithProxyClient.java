package com.ctrip.xpipe.redis.checker;

import com.ctrip.framework.xpipe.redis.ProxyRegistry;
import com.ctrip.framework.xpipe.redis.utils.ProxyUtil;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2022/2/9
 */
public class TestConnectProxyWithProxyClient extends AbstractCheckerIntegrationTest {

    private String dstIp = "127.0.0.1";

    private int dstPort = 6379;

    @Before
    public void setupTestConnectProxyWithProxyClient() {
        String routeInfo = "PROXY ROUTE PROXYTCP://10.0.0.1:7000 TCP";
        ProxyRegistry.registerProxy(dstIp, dstPort, routeInfo);
    }

    @BeforeClass
    public static void beforeClassTestConnectProxyWithProxyClient(){
        System.setProperty("DisableLoadProxyAgentJar", "false");
    }

    @Test
    public void testConnectFail_SocketRelease() throws Exception {
        Endpoint endpoint = new DefaultEndPoint(dstIp, dstPort);
        PingCommand command = new PingCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(endpoint), scheduled, 10000);
        try {
            command.execute().get();
        } catch (Exception e) {
            logger.info("[testConnectWithProxyClient] ping fail", e);
        }
        logger.info("[testConnectWithProxyClient] socketAddressSize {}", ProxyUtil.getInstance().usingProxySocketSize());
        waitConditionUntilTimeOut(() -> 0 == ProxyUtil.getInstance().usingProxySocketSize());
    }

}
