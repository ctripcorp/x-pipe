package com.ctrip.xpipe.redis.console.util;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.apache.velocity.VelocityContext;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 12, 2017
 */
public class VelocityUtilTest extends AbstractConsoleIntegrationTest {

    @Autowired
    VelocityUtil velocityUtil;

    @Test
    public void getRenderedString() throws Exception {
        String templateName = "VelocityTestTemplate.vm";
        VelocityContext context = new VelocityContext();
        String test = "Hello World!";
        context.put("test", test);

        String text = velocityUtil.getRenderedString(templateName, context);
        Assert.assertEquals(test, text);
    }

}