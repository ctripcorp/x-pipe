package com.ctrip.xpipe.redis.core.proxy.monitor;

import org.junit.Assert;
import org.junit.Test;

public class SessionTrafficResultTest {

    private SessionTrafficResult result;

    @Test
    public void toArray() {
        long timestamp = System.currentTimeMillis();
        long input = 1000, output = 2000, inputRates = 10, outputRates = 20;
        result = new SessionTrafficResult(timestamp, input, output, inputRates, outputRates);
        Object[] objects = result.toArray();
        Assert.assertEquals(timestamp, objects[0]);
        Assert.assertEquals(input, objects[1]);
        Assert.assertEquals(output, objects[2]);
    }

    @Test
    public void parseFromArray() {
        long timestamp = System.currentTimeMillis();
        long input = 1000, output = 2000, inputRates = 10, outputRates = 20;
        result = new SessionTrafficResult(timestamp, input, output, inputRates, outputRates);
        Object[] objects = result.toArray();
        SessionTrafficResult other = SessionTrafficResult.parseFromArray(objects);
        Assert.assertEquals(result, other);
    }

    @Test
    public void getTimestamp() {
        long timestamp = System.currentTimeMillis();
        long input = 1000, output = 2000, inputRates = 10, outputRates = 20;
        result = new SessionTrafficResult(timestamp, input, output, inputRates, outputRates);
        Assert.assertEquals(timestamp, result.getTimestamp());
    }

    @Test
    public void getInputBytes() {
        long timestamp = System.currentTimeMillis();
        long input = 1000, output = 2000, inputRates = 10, outputRates = 20;
        result = new SessionTrafficResult(timestamp, input, output, inputRates, outputRates);
        Assert.assertEquals(input, result.getInputBytes());
    }

    @Test
    public void getOutputBytes() {
        long timestamp = System.currentTimeMillis();
        long input = 1000, output = 2000, inputRates = 10, outputRates = 20;
        result = new SessionTrafficResult(timestamp, input, output, inputRates, outputRates);
        Assert.assertEquals(output, result.getOutputBytes());
    }
}