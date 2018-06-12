package com.ctrip.xpipe.email;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

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
    public CommandFuture<EmailResponse> sendEmailAsync(Email email) {
        return sendEmailAsync(email, MoreExecutors.directExecutor());
    }

    @Override
    public CommandFuture<EmailResponse> sendEmailAsync(Email email, Executor executor) {
        return new DefaultCommandFuture<>();
    }

    @Override
    public boolean checkAsyncEmailResult(EmailResponse response) {
        return true;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
