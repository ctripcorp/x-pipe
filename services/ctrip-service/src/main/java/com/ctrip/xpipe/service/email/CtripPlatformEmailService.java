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
    public void sendEmail(Email email) {
        CtripEmail ctripEmail = (CtripEmail) email;

        EmailServiceClient client = EmailServiceClient.getInstance();

        SendEmailRequest request = new SendEmailRequest();

        request.setAppID(ctripEmail.getAppID());
        request.setSender(ctripEmail.getSender());
        request.setRecipient(ctripEmail.getRecipients());
        request.setSendCode(ctripEmail.getSendCode());
        request.setSubject(ctripEmail.getSubject());
        request.setCharset(ctripEmail.getCharset());
        request.setBodyTemplateID(ctripEmail.getBodyTemplateID());
        request.setBodyContent(ctripEmail.getBodyContent());
        request.setIsBodyHtml(ctripEmail.isBodyHTML());

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR,1);
        request.setExpiredTime(calendar);

        try {
            SendEmailResponse response = client.sendEmail(request);
            if(response != null && response.getResultCode() == 1) {
                logger.info("Email sent successfully");
            } else if(response != null){
                logger.error("Email service Result message: {}", response.getResultMsg());
            }
        } catch (Exception e) {
            logger.error("Email service Error\n {}", e);
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
