package com.ctrip.xpipe.service.email;

import com.ctrip.soa.platform.basesystem.emailservice.v1.EmailServiceClient;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailRequest;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailResponse;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import jline.internal.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.concurrent.Callable;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class CtripPlatformEmailService implements EmailService {

    private static Logger logger = LoggerFactory.getLogger(CtripPlatformEmailService.class);

    private CatTransactionMonitor catTransactionMonitor = new CatTransactionMonitor();

    private static final String TYPE = "SOA.EMAIL.SERVICE";

    @Override
    public void sendEmail(Email email) {

        EmailServiceClient client = EmailServiceClient.getInstance();

        try {
            SendEmailResponse response = catTransactionMonitor.logTransaction(TYPE,
                    "send email", new Callable<SendEmailResponse>() {
                        @Override
                        public SendEmailResponse call() throws Exception {
                            return client.sendEmail(createSendEmailRequest(email));
                        }
                    });
            if(response != null && response.getResultCode() == 1) {
                logger.info("[sendEmail]Email sent successfully");
            } else if(response != null){
                logger.error("[sendEmail]Email service Result message: {}", response.getResultMsg());
            }
        } catch (Exception e) {
            logger.error("[sendEmail]Email service Error\n {}", e);
        }
    }

    private SendEmailRequest createSendEmailRequest(Email email) {

        CtripEmailTemplate ctripEmailTemplate = CtripEmailTemplateFactory
                .createCtripEmailTemplate(email.getEmailType());
        ctripEmailTemplate.decorateBodyContent(email);

        SendEmailRequest request = new SendEmailRequest();

        request.setSendCode(ctripEmailTemplate.getSendCode());
        request.setIsBodyHtml(ctripEmailTemplate.isBodyHTML());
        request.setAppID(ctripEmailTemplate.getAppID());
        request.setBodyTemplateID(ctripEmailTemplate.getBodyTemplateID());

        request.setSender(email.getSender());
        request.setRecipient(email.getRecipients());
        request.setCc(email.getCCers());
        request.setSubject(email.getSubject());
        request.setCharset(email.getCharset());
        request.setBodyContent(email.getBodyContent());

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, 1);
        request.setExpiredTime(calendar);
        return request;
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
