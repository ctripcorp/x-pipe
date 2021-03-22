package com.ctrip.xpipe.redis.checker.alert.sender.email;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.sender.email.listener.AsyncEmailSenderCallback;
import com.ctrip.xpipe.redis.checker.alert.sender.email.listener.EmailSendErrorReporter;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class AsyncEmailSenderTest extends AbstractCheckerIntegrationTest {

    @Autowired
    private AsyncEmailSender sender;

    @Test
    public void testInitListeners() throws Exception {
        AsyncEmailSenderCallback callbackFunction = sender.getCallbackFunction();
        Assert.assertNotNull(callbackFunction);
        Assert.assertTrue(callbackFunction instanceof EmailSendErrorReporter);
    }

    @Test
    public void testSend() throws Exception {
        sender.send(new AlertMessageEntity("Test", "test", Lists.newArrayList("test-list")));
    }

    @Test
    public void testCreateEventModel() {
        AlertMessageEntity message = new AlertMessageEntity("Test", "test", Lists.newArrayList("test-list"));
        message.setAlert(new AlertEntity(new HostPort("192.168.0.1", 1234), dcNames[0], "clusterId", "shardId",
                "message", ALERT_TYPE.CLIENT_INSTANCE_NOT_OK));

    }

}