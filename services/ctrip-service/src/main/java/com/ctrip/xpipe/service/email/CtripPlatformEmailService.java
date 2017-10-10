package com.ctrip.xpipe.service.email;

import com.ctrip.soa.platform.basesystem.emailservice.v1.EmailServiceClient;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailRequest;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailResponse;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class CtripPlatformEmailService implements EmailService {

    private static Logger logger = LoggerFactory.getLogger(CtripPlatformEmailService.class);

    @Override
    public <T extends Email> void sendEmail(T t, Object... context) {
        AbstractEmail email = (AbstractEmail) t;

        EmailServiceClient client = EmailServiceClient.getInstance();

        SendEmailRequest request = new SendEmailRequest();

        request.setAppID(email.getAppID());
        request.setSender(email.getSender());
        request.setRecipient(email.getRecipients());
        request.setSendCode(email.getSendCode());
        request.setSubject(email.getSubject());
        request.setCharset(email.getCharset());
        request.setBodyTemplateID(email.getBodyTemplateID());
        request.setBodyContent(email.getBodyContent(context));
        request.setIsBodyHtml(email.isBodyHTML());

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR,1);
        request.setExpiredTime(calendar);

        try {
            SendEmailResponse response = client.sendEmail(request);
            logger.info("Email service ResultCode: {}, Result message: {}",
                    response.getResultCode(), response.getResultMsg());
        } catch (Exception e) {
            logger.error("Email service Error\n {}", e);
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
