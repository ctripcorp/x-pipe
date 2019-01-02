package com.ctrip.xpipe.redis.core.proxy.monitor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TunnelTrafficResultTest {

    private TunnelTrafficResult result;

    private String tunnelId = "";

    private SessionTrafficResult frontend;

    private SessionTrafficResult backend;

    @Before
    public void beforeTunnelTrafficResultTest() {
        tunnelId = "TCP://127.0.0.1:8080-PROXYTCP:127.0.0.1:8080";
        long timestamp = System.currentTimeMillis();
        long input = 1000, output = 2000, inputRates = 10, outputRates = 20;
        frontend = new SessionTrafficResult(timestamp, input, output, inputRates, outputRates);
        backend = new SessionTrafficResult(timestamp, input + 100, output + 200, inputRates, outputRates);
        result = new TunnelTrafficResult(tunnelId, frontend, backend);
    }

    @Test
    public void format() {
        Object objects = result.format();
        Assert.assertTrue(objects instanceof Object[]);
        Object[] data = (Object[]) objects;
        Assert.assertEquals(3, data.length);
        Assert.assertEquals(tunnelId, data[0]);
        Assert.assertEquals(frontend, SessionTrafficResult.parseFromArray((Object[]) data[1]));
        Assert.assertEquals(backend, SessionTrafficResult.parseFromArray((Object[]) data[2]));
    }

    @Test
    public void parse() {
        Object objects = result.format();
        TunnelTrafficResult other = TunnelTrafficResult.parse(objects);
        Assert.assertEquals(result, other);
        Assert.assertEquals(frontend, other.getFrontend());
        Assert.assertEquals(backend, other.getBackend());
    }

    @Test
    public void getTunnelId() {
        Assert.assertEquals(tunnelId, result.getTunnelId());
    }

    @Test
    public void getFrontend() {
        Assert.assertEquals(frontend, result.getFrontend());
    }

    @Test
    public void getBackend() {
        Assert.assertEquals(backend, result.getBackend());
    }
}