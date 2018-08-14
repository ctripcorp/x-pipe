package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.Sender;
import com.ctrip.xpipe.redis.console.alert.sender.email.AsyncEmailSender;
import com.ctrip.xpipe.redis.console.alert.sender.email.EmailSender;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public class SenderManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    SenderManager senderManager;

    @Before
    public void beforeSenderManagerTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void querySender() throws Exception {
        Sender sender = senderManager.querySender(EmailSender.ID);
        Assert.assertTrue(sender instanceof EmailSender);
        AlertChannel channel = AlertChannel.MAIL;
        String id = channel.getId();
        sender = senderManager.querySender(id);
        Assert.assertTrue(sender instanceof AsyncEmailSender);
    }

    @Test
    public void sendAlert() throws Exception {
        String title = "Test";
        String content = "Test Content";
        List<String> recepients = Arrays.asList("test1@gmail.com, test2@gmail.com");
        senderManager.sendAlert(AlertChannel.MAIL,
                new AlertMessageEntity(title, content, recepients));
    }

}