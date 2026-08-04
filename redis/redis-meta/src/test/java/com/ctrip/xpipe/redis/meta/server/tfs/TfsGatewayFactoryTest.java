package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import org.junit.Assert;
import org.junit.Test;

public class TfsGatewayFactoryTest {

    @Test
    public void testMockHost() {
        Assert.assertTrue(TfsGatewayFactory.create("mock://tfs-gateway") instanceof MockTfsGateway);
        Assert.assertTrue(TfsGatewayFactory.create("") instanceof MockTfsGateway);
        Assert.assertTrue(TfsGatewayFactory.isMockHost(null));
    }

    @Test
    public void testMockHostFromConfig() {
        UnitTestServerConfig config = new UnitTestServerConfig().setTfsGatewayHost("mock://tfs-gateway");
        Assert.assertTrue(TfsGatewayFactory.create(config) instanceof MockTfsGateway);
    }

    @Test
    public void testSameMockHostReturnsSameInstance() {
        TfsGateway first = TfsGatewayFactory.create("mock://tfs-gateway");
        TfsGateway second = TfsGatewayFactory.create("mock://other");
        Assert.assertSame(first, second);
    }

    @Test
    public void testRealHostReturnsHttpGateway() {
        UnitTestServerConfig config = new UnitTestServerConfig()
                .setTfsGatewayHost("http://real-tfs-gateway")
                .setTfsGatewayAppId(100004376L);
        TfsGateway gateway = TfsGatewayFactory.create(config);
        Assert.assertTrue(gateway instanceof HttpTfsGateway);
        Assert.assertSame(config, ((HttpTfsGateway) gateway).getConfig());
    }

    @Test
    public void testRealHostStringCreatesHttpGateway() {
        TfsGateway gateway = TfsGatewayFactory.create("http://real-tfs-gateway");
        Assert.assertTrue(gateway instanceof HttpTfsGateway);
        Assert.assertNull(((HttpTfsGateway) gateway).getConfig());
    }
}
