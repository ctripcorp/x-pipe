package com.ctrip.xpipe.redis.console.alert.sender.email;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.console.alert.sender.AbstractSender;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.AsyncEmailSenderCallback;
import com.ctrip.xpipe.redis.console.alert.sender.email.listener.EmailSendErrorReporter;
import com.ctrip.xpipe.redis.console.model.EventModel;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    private static final Logger logger = LoggerFactory.getLogger(AsyncEmailSender.class);

    public static final String ID = "com.ctrip.xpipe.console.alert.email.async.sender";

    private AsyncEmailSenderCallback callbackFunction;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executor;

    @Autowired
    private AlertEventService alertEventService;

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
            alertEventService.insert(createEventModel(message, response));
        });
        if(future.isDone() && !future.isSuccess()) {
            callbackFunction.fail(future.cause());
            return false;
        }
        return true;
    }

    @VisibleForTesting
    @SuppressWarnings("unchecked")
    protected EventModel createEventModel(AlertMessageEntity message, EmailResponse response) {
        EventModel model = new EventModel();
        model.setEventType(EventModel.EventType.ALERT_EMAIL).setEventOperator(FoundationService.DEFAULT.getLocalIp())
                .setEventDetail(message.getTitle());
        if(message.getAlert() != null) {
            model.setEventOperation(message.getAlert().getAlertType().name());
        } else {
            model.setEventOperation("grouped");
        }
        String emailCheckInfo = null;
        try {
            emailCheckInfo = JsonCodec.INSTANCE.encode(response.getProperties());
        } catch (Exception e) {
            logger.error("[createEventModel] Error encode check info");
        }
        model.setEventProperty(emailCheckInfo);
        return model;
    }

    @VisibleForTesting
    public AsyncEmailSenderCallback getCallbackFunction() {
        return callbackFunction;
    }
}
