package com.ctrip.xpipe.redis.meta.server.tfs;

import org.junit.Assert;
import org.junit.Test;

public class TfsGatewayFactoryTest {

    @Test
    public void testMockEndpoint() {
        Assert.assertTrue(TfsGatewayFactory.create("mock://tfs-gateway") instanceof MockTfsGateway);
    }

    @Test
    public void testSameEndpointReturnsCachedInstance() {
        TfsGateway first = TfsGatewayFactory.create("mock://tfs-gateway");
        TfsGateway second = TfsGatewayFactory.create("mock://tfs-gateway");
        Assert.assertSame(first, second);
    }

    @Test
    public void testUnknownEndpointDoesNotThrowOnCreate() {
        TfsGateway gateway = TfsGatewayFactory.create("http://real-tfs-gateway");
        Assert.assertNotNull(gateway);
        try {
            gateway.forceCloseDir("fs-1", "/path");
            Assert.fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            // expected on invoke, not on create
        } catch (Exception e) {
            Assert.fail("unexpected exception: " + e);
        }
    }
}
