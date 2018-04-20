package com.ctrip.xpipe.redis.console.alert.sender;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.manager.SenderManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public abstract class AbstractSender implements Sender {

    @Autowired
    private SenderManager senderManager;

    @Autowired
    private ConsoleConfig consoleConfig;

    public static final String CC_ER = "ccers";

    public Sender querySender() {
        String id = getId();
        return senderManager.querySender(id);
    }

    protected Email createEmail(AlertMessageEntity message) {
        Email email = new Email();
        email.setSender(consoleConfig.getRedisAlertSenderEmail());
        email.setSubject(message.getTitle());
        email.setCharset("UTF-8");

        List<String> ccers = message.getParam(CC_ER);
        if(ccers != null && !ccers.isEmpty()) {
            email.setcCers(ccers);
        }
        email.setRecipients(message.getReceivers());
        email.setBodyContent(message.getContent());
        return email;
    }
}
