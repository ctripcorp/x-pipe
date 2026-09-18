package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.ActionContextRetMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class InfoActionContextTest {

    private static final String RAW =
        "# Server\r\n" +
        "redis_version:7.0.0\r\n" +
        "uptime_in_seconds:100\r\n" +
        "# Replication\r\n" +
        "role:master\r\n" +
        "connected_slaves:2\r\n" +
        "# Memory\r\n" +
        "used_memory:1024\r\n";

    @Test
    public void parseNullSection_returnsAll() {
        Map<String, String> m = InfoActionContext.parse(RAW, null);
        Assert.assertEquals(5, m.size());
        Assert.assertEquals("master", m.get("role"));
    }

    @Test
    public void parseEmptySection_returnsAll() {
        Map<String, String> m = InfoActionContext.parse(RAW, "");
        Assert.assertEquals(5, m.size());
    }

    @Test
    public void parseReplicationSection_returnsOnlyReplication() {
        Map<String, String> m = InfoActionContext.parse(RAW, "replication");
        Assert.assertEquals(2, m.size());
        Assert.assertEquals("master", m.get("role"));
        Assert.assertEquals("2", m.get("connected_slaves"));
        Assert.assertNull(m.get("redis_version"));
    }

    @Test
    public void parseSectionCaseInsensitive() {
        Map<String, String> m = InfoActionContext.parse(RAW, "REPLICATION");
        Assert.assertEquals(2, m.size());
    }

    @Test
    public void parseUnknownSection_returnsEmpty() {
        Map<String, String> m = InfoActionContext.parse(RAW, "foo");
        Assert.assertTrue(m.isEmpty());
    }

    @Test
    public void toRetMessage_successWithSection_returnsFilteredPayload() {
        RawInfoActionContext raw = new RawInfoActionContext(null, RAW);
        InfoActionContext ctx = () -> raw;
        InfoActionContext.Result ret = InfoActionContext.toRetMessage(ctx, "replication");
        Assert.assertEquals(ActionContextRetMessage.SUCCESS_STATE, ret.getState());
        @SuppressWarnings("unchecked")
        Map<String, String> payload = (Map<String, String>) ret.getPayload();
        Assert.assertEquals(2, payload.size());
        Assert.assertEquals("master", payload.get("role"));
        Assert.assertEquals("2", payload.get("connected_slaves"));
        Assert.assertNull(payload.get("redis_version"));
    }

    @Test
    public void toRetMessage_successNullSection_returnsAllPayload() {
        RawInfoActionContext raw = new RawInfoActionContext(null, RAW);
        InfoActionContext ctx = () -> raw;
        InfoActionContext.Result ret = InfoActionContext.toRetMessage(ctx, null);
        Assert.assertEquals(ActionContextRetMessage.SUCCESS_STATE, ret.getState());
        @SuppressWarnings("unchecked")
        Map<String, String> payload = (Map<String, String>) ret.getPayload();
        Assert.assertEquals(5, payload.size());
    }

    @Test
    public void toRetMessage_fail_setsFailStateAndMessage() {
        RuntimeException cause = new RuntimeException("boom");
        RawInfoActionContext rawFail = new RawInfoActionContext(null, cause);
        InfoActionContext failCtx = () -> rawFail;
        InfoActionContext.Result ret = InfoActionContext.toRetMessage(failCtx, "replication");
        Assert.assertEquals(ActionContextRetMessage.FAIL_STATE, ret.getState());
        Assert.assertEquals("boom", ret.getMessage());
        Assert.assertNull(ret.getPayload());
    }

    @Test
    public void toRetMessage_mapAggregatesAll() {
        RawInfoActionContext raw = new RawInfoActionContext(null, RAW);
        InfoActionContext ctx = () -> raw;
        Map<HostPort, InfoActionContext> contexts = new HashMap<>();
        contexts.put(new HostPort("1.1.1.1", 6379), ctx);
        InfoActionContext.ResultMap result = InfoActionContext.toRetMessage(contexts, "replication");
        Assert.assertEquals(1, result.size());
        InfoActionContext.Result ret = (InfoActionContext.Result) result.get(new HostPort("1.1.1.1", 6379));
        Assert.assertEquals(ActionContextRetMessage.SUCCESS_STATE, ret.getState());
        @SuppressWarnings("unchecked")
        Map<String, String> payload = (Map<String, String>) ret.getPayload();
        Assert.assertEquals(2, payload.size());
    }
}

