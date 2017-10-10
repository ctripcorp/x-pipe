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
    public <T extends Email> void sendEmail(T t, Object... context) {
        logger.info("Sender: {}", t.getSender());
        logger.info("Receivers: {}", t.getRecipients());
        logger.info("CCers: {}", t.getCCers());
        logger.info("BCCers: {}", t.getBCCers());
        logger.info("Context:\n{}", context);
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
