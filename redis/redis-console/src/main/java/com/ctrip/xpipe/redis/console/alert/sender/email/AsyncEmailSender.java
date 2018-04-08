package com.ctrip.xpipe.redis.console.alert.sender.email;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.AbstractSender;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.AsyncEmailSenderCallback;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.CompositeEmailSenderCallback;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.EmailSendErrorReporter;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.EmailSentCounter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */

@Component(AsyncEmailSender.ID)
public class AsyncEmailSender extends AbstractSender {

    public static final String ID = "com.ctrip.xpipe.console.alert.email.async.sender";

    private AsyncEmailSenderCallback callbackFunction;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    @Override
    public String getId() {
        return ID;
    }

    @PostConstruct
    public void initListeners() {
        CompositeEmailSenderCallback compositeEmailSenderCallback = new CompositeEmailSenderCallback();
        compositeEmailSenderCallback.register(new EmailSendErrorReporter());
        compositeEmailSenderCallback.register(new EmailSentCounter());
        callbackFunction = compositeEmailSenderCallback;
    }

    @Override
    public boolean send(AlertMessageEntity message) {
        CommandFuture<Void> future = EmailService.DEFAULT.sendEmailAsync(createEmail(message), executor);
        future.addListener(new CommandFutureListener<Void>() {
            @Override
            public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
                if(commandFuture.isSuccess()) {
                    callbackFunction.success();
                } else {
                    callbackFunction.fail(commandFuture.cause());
                }

            }
        });
        if(future.isDone() && !future.isSuccess()) {
            return false;
        }
        return true;
    }

    @VisibleForTesting
    public AsyncEmailSenderCallback getCallbackFunction() {
        return callbackFunction;
    }
}
