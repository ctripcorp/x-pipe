package com.ctrip.xpipe.redis.console.alert.sender.email;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.AbstractSender;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component(EmailSender.ID)
public class EmailSender extends AbstractSender {

    public static final String ID = "com.ctrip.xpipe.console.alert.sender.email.sender";

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

}
