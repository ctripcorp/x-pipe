package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.codec.JsonCodec;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2022/4/8
 */
public class RedisCheckRuleTest extends AbstractTest {

    private String jsonStr = "{\"checkType\":\"crdt.config\",\"params\":{\"configCheckName\":\"repl-backlog-size\",\"expectedVaule\":\"268435456\"}}";

    @Test
    public void jsonDecodeTest() {
        RedisCheckRule checkRule = JsonCodec.DEFAULT.decode(jsonStr, RedisCheckRule.class);
        Assert.assertEquals("crdt.config", checkRule.getCheckType());
        Assert.assertEquals("repl-backlog-size", checkRule.getParams().get("configCheckName"));
        Assert.assertEquals("268435456", checkRule.getParams().get("expectedVaule"));
    }

}
