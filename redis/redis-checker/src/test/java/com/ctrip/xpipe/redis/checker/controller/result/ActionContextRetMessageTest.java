package com.ctrip.xpipe.redis.checker.controller.result;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo.InfoActionContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Slight
 * <p>
 * Jun 07, 2021 1:53 AM
 */
public class ActionContextRetMessageTest {

    @Test
    public void fixHttpMessageNotReadableException() {
        String json = "{\"10.6.57.204:21834\" : {\n" +
                "    \"state\" : 0,\n" +
                "    \"message\" : null,\n" +
                "    \"payload\" : {\n" +
                "      \"second_repl_offset\" : \"1775645783\",\n" +
                "      \"repl_backlog_first_byte_offset\" : \"2294554294\",\n" +
                "      \"role\" : \"master\",\n" +
                "      \"repl_backlog_active\" : \"1\",\n" +
                "      \"repl_backlog_size\" : \"1048576\",\n" +
                "      \"connected_slaves\" : \"2\",\n" +
                "      \"slave0\" : \"ip=10.6.57.204,port=21835,state=online,offset=2295602869,lag=1\",\n" +
                "      \"repl_backlog_histlen\" : \"1048576\",\n" +
                "      \"slave1\" : \"ip=10.6.57.192,port=7297,state=online,offset=2295602869,lag=1\",\n" +
                "      \"master_replid\" : \"6b2e2174db5391023e068415c1d1b80c896f3bda\",\n" +
                "      \"master_replid2\" : \"bfbcaf7376353f311bfb9170ee7297c712692b7e\",\n" +
                "      \"master_repl_offset\" : \"2295602869\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"10.6.57.204:21833\" : {\n" +
                "    \"state\" : -1,\n" +
                "    \"message\" : \"remote closed:L(null)->R(null)\",\n" +
                "    \"payload\" : null\n" +
                "  }" +
                "}";
        InfoActionContext.ResultMap result = JsonCodec.DEFAULT.decode(json, InfoActionContext.ResultMap.class);
        assertEquals("remote closed:L(null)->R(null)", result.get(HostPort.fromString("10.6.57.204:21833")).getMessage());
    }
}