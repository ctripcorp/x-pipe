package com.ctrip.xpipe.service.email;

import com.ctrip.soa.platform.basesystem.emailservice.v1.*;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ctrip.xpipe.service.email.CtripAlertEmailTemplate.EMAIL_TYPE_ALERT;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class CtripPlatformEmailService implements EmailService {

    private static Logger logger = LoggerFactory.getLogger(CtripPlatformEmailService.class);

    private CatTransactionMonitor catTransactionMonitor = new CatTransactionMonitor();

    private static final String TYPE = "SOA.EMAIL.SERVICE";

    private static EmailServiceClient client = EmailServiceClient.getInstance();

    @Override
    public void sendEmail(Email email) {

        try {
            SendEmailResponse response = catTransactionMonitor.logTransaction(TYPE,
                    "send email", new Callable<SendEmailResponse>() {
                        @Override
                        public SendEmailResponse call() throws Exception {
                            return client.sendEmail(createSendEmailRequest(email));
                        }
                    });

            if(response != null && response.getResultCode() == 1) {
                logger.debug("[sendEmail]Email send out successfully");
            } else if(response != null){
                logger.error("[sendEmail]Email service Result message: {}", response.getResultMsg());
                throw new XpipeRuntimeException(response.getResultMsg());
            }

        } catch (Exception e) {
            logger.error("[sendEmail]Email service Error\n", e);
        }
    }

    private static final Pattern VALID_EMAIL_ADDRESS_REGEX =
            Pattern.compile("^[A-Z0-9._%+-]+@C?trip.com$", Pattern.CASE_INSENSITIVE);

    @Override
    public CheckEmailResponse checkEmailAddress(String address) {
        Matcher matcher = VALID_EMAIL_ADDRESS_REGEX.matcher(address);
        boolean result = matcher.find();
        if(result) {
            return new CheckEmailResponse(true);
        } else {
            return new CheckEmailResponse(false, "Emails should be ctrip/trip emails and separated by comma or semicolon");
        }
    }

    @Override
    public CommandFuture<EmailResponse> sendEmailAsync(Email email) {
        return sendEmailAsync(email, MoreExecutors.directExecutor());
    }

    @Override
    public CommandFuture<EmailResponse> sendEmailAsync(Email email, Executor executor) {
        return new AsyncSendEmailCommand(email).execute(executor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean checkAsyncEmailResult(EmailResponse response) {
        try {
            String emailIDListStr = (String) response.getProperties().get(EmailResponse.KEYS.CHECK_INFO.name());
            List<String> emailIDList = decodeListString(emailIDListStr);
            GetEmailStatusResponse emailStatusResponse = client.getEmailStatus(
                    new GetEmailStatusRequest(CtripAlertEmailTemplate.SEND_CODE, emailIDList, EMAIL_TYPE_ALERT));

            logger.debug("[checkAsyncEmailResult]Email sent out result: {}", emailStatusResponse);
            return emailStatusResponse.getResultCode() == 1;
        }catch (Exception e) {
            logger.error("check email send response error: ", e);
        }
        return false;
    }


    private static SendEmailRequest createSendEmailRequest(Email email) {

        CtripEmailTemplate ctripEmailTemplate = CtripEmailTemplateFactory.createCtripEmailTemplate();
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


    static class CtripEmailResponse implements EmailResponse {

        private Properties properties = new Properties();

        public CtripEmailResponse(SendEmailResponse response) {
            properties.put(KEYS.CHECK_INFO.name(), encodeListString(response.getEmailIDList()));
        }

        @Override
        public Properties getProperties() {
            return properties;
        }
    }

    static class AsyncSendEmailCommand extends AbstractCommand<EmailResponse> {

        private Email email;

        public AsyncSendEmailCommand(Email email) {
            this.email = email;
        }

        @Override
        public String getName() {
            return "email-send-command";
        }

        @Override
        protected void doExecute() {
            try {
                SendEmailResponse response = client.sendEmail(createSendEmailRequest(email));
                if (response == null || response.getResultCode() != 1) {
                    String message = response == null ? "no response from email service" : response.getResultMsg();
                    logger.error("[SendEmailResponse] code: {}, message: {}", response.getResultCode(), response.getResultMsg());
                    future().setFailure(new XpipeRuntimeException(message));
                    return;
                }

                future().setSuccess(new CtripEmailResponse(response));
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @VisibleForTesting
        public static void setClient(EmailServiceClient c) {
            client = c;
        }
    }

    @VisibleForTesting
    protected static String encodeListString(List<String> strs) {
        StringBuffer sb = new StringBuffer();
        for(String str : strs) {
            sb.append(str).append(",");
        }
        if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    @VisibleForTesting
    protected static List<String> decodeListString(String str) {
        String[] strs = StringUtil.splitRemoveEmpty("\\s*,\\s*", str);
        return Arrays.asList(strs);
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
