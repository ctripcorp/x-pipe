package com.ctrip.xpipe.redis.checker.alert.sender;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.manager.SenderManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
public abstract class AbstractSender implements Sender {

    @Autowired
    private AlertConfig alertConfig;

    public static final String CC_ER = "ccers";


    protected Email createEmail(AlertMessageEntity message) {
        Email email = new Email();
        email.setSender(alertConfig.getRedisAlertSenderEmail());
        email.setSubject(message.getTitle());
        email.setCharset("UTF-8");

        List<String> ccers = message.getParam(CC_ER);
        if(ccers != null && !ccers.isEmpty()) {
            email.setcCers(ccers);
        }
        email.setRecipients(message.getReceivers());
        // 确保bodyContent不为空
        String content = message.getContent();
        if(content == null || content.trim().isEmpty()) {
            content = "Alert notification - no content available";
        }
        email.setBodyContent(content);
        return email;
    }
}
