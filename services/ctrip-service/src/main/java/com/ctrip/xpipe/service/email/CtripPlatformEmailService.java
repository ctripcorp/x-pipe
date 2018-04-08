package com.ctrip.xpipe.service.email;

import com.ctrip.soa.platform.basesystem.emailservice.v1.*;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.monitor.CatTransactionMonitor;
import com.ctrip.xpipe.retry.RetryNTimes;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.concurrent.*;

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

    private static final ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor(
            XpipeThreadFactory.create(CtripPlatformEmailService.class.getSimpleName()));

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
            logger.error("[sendEmail]Email service Error\n {}", e);
            Throwable th = e;
            while(th.getCause() instanceof XpipeRuntimeException) {
                th = th.getCause();
            }
            throw new XpipeRuntimeException(th.getMessage());
        }
    }

    @Override
    public CommandFuture<Void> sendEmailAsync(Email email) {
        return sendEmailAsync(email, MoreExecutors.directExecutor());
    }

    @Override
    public CommandFuture<Void> sendEmailAsync(Email email, Executor executor) {
        return new AsyncSendEmailCommand(email).execute(executor);
    }

    private static void checkSentResult(SendEmailResponse response) throws Exception {

        GetEmailStatusResponse emailStatusResponse = client.getEmailStatus(
                new GetEmailStatusRequest(CtripAlertEmailTemplate.SEND_CODE, response.getEmailIDList()));

        if(emailStatusResponse.getResultCode() != 1) {
            throw new XpipeRuntimeException(emailStatusResponse.getResultMsg());
        }

    }


    private static SendEmailRequest createSendEmailRequest(Email email) {

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


    static class AsyncSendEmailCommand extends AbstractCommand<Void> {

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

                // Retry 5 time, 2 sec for each, waiting for email sent result
                RetryCommandFactory factory = DefaultRetryCommandFactory.retryNTimes(scheduled, 3, (int) TimeUnit.MINUTES.toMillis(1));
                Command<Void> retryCommand = factory.createRetryCommand(new AsyncCheckEmailCommand(response));
                CommandFuture<Void> future = retryCommand.execute();
                future.addListener(commandFuture -> {
                    if(commandFuture.isSuccess()) {
                        future().setSuccess();
                    } else {
                        logger.error("[EmailCheckCommandListener] success: {}, cause: {}, email id list: {}",
                                commandFuture.isSuccess(), commandFuture.cause(), response.getEmailIDList());

                        future().setFailure(commandFuture.cause());
                    }
                });
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

    static class AsyncCheckEmailCommand extends AbstractCommand<Void> {

        private SendEmailResponse response;

        public AsyncCheckEmailCommand(SendEmailResponse response) {
            this.response = response;
        }


        @Override
        protected void doExecute() throws Exception {
            try {
                checkSentResult(response);
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return "check-email-send";
        }
    }



    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
