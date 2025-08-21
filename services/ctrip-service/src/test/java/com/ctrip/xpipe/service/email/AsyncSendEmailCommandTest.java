package com.ctrip.xpipe.service.email;

import com.ctrip.soa.platform.basesystem.emailservice.v1.EmailServiceClient;
import com.ctrip.soa.platform.basesystem.emailservice.v1.GetEmailStatusResponse;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailResponse;
import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.utils.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public class AsyncSendEmailCommandTest extends AbstractTest {

    @Mock
    EmailServiceClient client;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testAsyncSendEmailCommand() throws Exception {
        when(client.sendEmail(any())).thenThrow(new XpipeException("test exception"));

        CtripPlatformEmailService.AsyncSendEmailCommand command = new CtripPlatformEmailService
                .AsyncSendEmailCommand(generateEmail());

        command.setClient(client);

        CommandFuture future = command.execute();

        Assert.assertFalse(future.isSuccess());

        Assert.assertEquals("test exception", future.cause().getMessage());
    }

    @Test
    public void testAsyncSendEmailCommand2() throws Exception {

        SendEmailResponse response = new SendEmailResponse();
        response.setEmailIDList(Collections.emptyList());
        response.setResultCode(1);
        when(client.sendEmail(any())).thenReturn(response);

        GetEmailStatusResponse getResponse = new GetEmailStatusResponse();
        getResponse.setResultCode(1);
        when(client.getEmailStatus(any())).thenReturn(getResponse);

        CtripPlatformEmailService.AsyncSendEmailCommand command = new CtripPlatformEmailService
                .AsyncSendEmailCommand(generateEmail());

        command.setClient(client);

        CommandFuture future = command.execute();

        future.await(10000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(future.isSuccess());
    }

    @Test
    public void testAsyncSendEmailCommand3() throws Exception {

        SendEmailResponse response = new SendEmailResponse();
        response.setResultCode(1);
        when(client.sendEmail(any())).thenReturn(response);

        GetEmailStatusResponse getResponse = new GetEmailStatusResponse();
        getResponse.setResultCode(0);
        getResponse.setResultMsg("test exception result could be caught");
        when(client.getEmailStatus(any())).thenReturn(getResponse);

        CtripPlatformEmailService.AsyncSendEmailCommand command = new CtripPlatformEmailService
                .AsyncSendEmailCommand(generateEmail());

        command.setClient(client);

        CommandFuture future = command.execute();

        future.addListener(commandFuture -> {
            Assert.assertFalse(future.isSuccess());
            Assert.assertEquals("test exception result could be caught", future.cause().getMessage());
        });

        waitConditionUntilTimeOut(()->future.isDone(), 10 * 1000);
    }

    @Test
    public void testAsyncSendEmailCommand4() throws Exception {
        SendEmailResponse response = new SendEmailResponse();
        response.setResultCode(0);
        response.setResultMsg("no recipient from email");
        when(client.sendEmail(any())).thenReturn(response);

        CtripPlatformEmailService.AsyncSendEmailCommand command = new CtripPlatformEmailService
                .AsyncSendEmailCommand(generateEmail());

        command.setClient(client);

        CommandFuture future = command.execute();

        Assert.assertFalse(future.isSuccess());

        Assert.assertEquals("no recipient from email", future.cause().getMessage());
    }

    private Email generateEmail() throws IOException {
        String path = "src/test/resources/ctripPlatformEmailServiceTest.txt";
        InputStream ins = FileUtils.getFileInputStream(path);
        String text = IOUtils.toString(ins);
        Email email = new Email();
        email.setBodyContent(text);
        email.addRecipient("zhuchen@ctrip.com");
        email.setSender("xpipe@test.com");
        email.setSubject("XPipe Subject");
        return email;
    }

}