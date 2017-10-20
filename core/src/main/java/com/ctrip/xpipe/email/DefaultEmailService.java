package com.ctrip.xpipe.email;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class DefaultEmailService implements EmailService {

    private final static Logger logger = LoggerFactory.getLogger(DefaultEmailService.class);

    @Override
    public void sendEmail(Email email) {
        logger.info("Sender: {}", email.getSender());
        logger.info("Receivers: {}", email.getRecipients());
        logger.info("CCers: {}", email.getCCers());
        logger.info("BCCers: {}", email.getBCCers());
        logger.info("Context:\n{}", email.getBodyContent());
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
