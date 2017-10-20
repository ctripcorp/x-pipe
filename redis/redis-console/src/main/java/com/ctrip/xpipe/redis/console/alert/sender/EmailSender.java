package com.ctrip.xpipe.redis.console.alert.sender;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component(EmailSender.ID)
public class EmailSender extends AbstractSender {

    public static final String ID = "com.ctrip.xpipe.console.alert.sender.email.sender";

    public static final String CC_ER = "ccers";

    @Autowired
    private ConsoleConfig consoleConfig;


    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean send(AlertMessageEntity message) {
        Email email = createEmail(message);
        try {
            EmailService.DEFAULT.sendEmail(email);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    private Email createEmail(AlertMessageEntity message) {
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
        email.setEmailType(message.getType());
        return email;
    }


}
