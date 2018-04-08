package com.ctrip.xpipe.redis.console.alert.sender.email;

import com.ctrip.xpipe.api.email.EmailType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.AsyncEmailSenderCallback;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.CompositeEmailSenderCallback;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class AsyncEmailSenderTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private AsyncEmailSender sender;

    @Test
    public void testInitListeners() throws Exception {
        AsyncEmailSenderCallback callbackFunction = sender.getCallbackFunction();
        Assert.assertNotNull(callbackFunction);
        Assert.assertTrue(callbackFunction instanceof CompositeEmailSenderCallback);
        List<AsyncEmailSenderCallback> callbacks = ((CompositeEmailSenderCallback) callbackFunction).getCallbacks();

        logger.info("{}", callbacks);
    }

    @Test
    public void testSend() throws Exception {
        sender.send(new AlertMessageEntity("Test", EmailType.CONSOLE_ALERT, "test", Lists.newArrayList("test-list")));
    }

}