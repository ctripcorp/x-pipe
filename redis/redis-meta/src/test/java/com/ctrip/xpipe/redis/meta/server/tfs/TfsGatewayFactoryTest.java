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
    public void testRealHostReturnsUnimplementedPlaceholder() {
        UnitTestServerConfig config = new UnitTestServerConfig()
                .setTfsGatewayHost("http://real-tfs-gateway")
                .setTfsGatewayAppId(100004376L);
        TfsGateway gateway = TfsGatewayFactory.create(config);
        Assert.assertTrue(gateway instanceof TfsGatewayFactory.UnimplementedTfsGateway);
        TfsGatewayFactory.UnimplementedTfsGateway unimplemented =
                (TfsGatewayFactory.UnimplementedTfsGateway) gateway;
        Assert.assertSame(config, unimplemented.getConfig());
        try {
            gateway.forceCloseDir("fs-1", "/path", "10.0.0.1");
            Assert.fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            Assert.assertTrue(expected.getMessage().contains("http://real-tfs-gateway"));
            Assert.assertTrue(expected.getMessage().contains("100004376"));
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e);
        }
    }

    @Test
    public void testUnimplementedReadsLatestHostFromConfig() {
        UnitTestServerConfig config = new UnitTestServerConfig()
                .setTfsGatewayHost("http://host-a")
                .setTfsGatewayAppId(1L);
        TfsGateway gateway = TfsGatewayFactory.create(config);
        config.setTfsGatewayHost("http://host-b").setTfsGatewayAppId(2L);
        try {
            gateway.forceCloseDir("fs-1", "/path", "10.0.0.1");
            Assert.fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            Assert.assertTrue(expected.getMessage().contains("http://host-b"));
            Assert.assertTrue(expected.getMessage().contains("appId=2"));
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e);
        }
    }
}
