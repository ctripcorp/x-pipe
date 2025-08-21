package com.ctrip.xpipe.redis.checker.alert.sender.email;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.alert.sender.AbstractSender;
import com.ctrip.xpipe.redis.checker.alert.sender.email.listener.AsyncEmailSenderCallback;
import com.ctrip.xpipe.redis.checker.alert.sender.email.listener.EmailSendErrorReporter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */

@Component(AsyncEmailSender.ID)
public class AsyncEmailSender extends AbstractSender {

    private static final Logger logger = LoggerFactory.getLogger(AsyncEmailSender.class);

    public static final String ID = "com.ctrip.xpipe.console.alert.email.async.sender";

    private AsyncEmailSenderCallback callbackFunction;

    @Resource
    FoundationService foundationService;
    
    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    @Autowired
    private PersistenceCache persistenceCache;

    @Override
    public String getId() {
        return ID;
    }

    @PostConstruct
    public void initListeners() {
        callbackFunction = new EmailSendErrorReporter();
    }

    @Override
    public boolean send(AlertMessageEntity message) {
        CommandFuture<EmailResponse> future = EmailService.DEFAULT.sendEmailAsync(createEmail(message), executor);
        future.addListener(commandFuture -> {
            EmailResponse response = commandFuture.getNow();
            persistenceCache.recordAlert(foundationService.getLocalIp(), message, response);
        });
        if(future.isDone() && !future.isSuccess()) {
            callbackFunction.fail(future.cause());
            return false;
        }
        return true;
    }

    @VisibleForTesting
    public AsyncEmailSenderCallback getCallbackFunction() {
        return callbackFunction;
    }
}
