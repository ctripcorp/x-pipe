package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/21
 */
public class RedisUpdateControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RedisUpdateController controller;

    @Test
    public void getRedises() {
        Map<String, String> result = controller.getRedises("jq", "cluster1", "shard1");
        Assert.assertEquals(new HashMap<String, String>() {{
            put("127.0.0.1:6379", Server.SERVER_ROLE.MASTER.name());
            put("127.0.0.1:6479", Server.SERVER_ROLE.SLAVE.name());
        }}, result);
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

}
