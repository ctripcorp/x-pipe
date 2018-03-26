package com.ctrip.xpipe.service.email;

import com.ctrip.soa.platform.basesystem.emailservice.v1.EmailServiceClient;
import com.ctrip.soa.platform.basesystem.emailservice.v1.GetEmailStatusResponse;
import com.ctrip.soa.platform.basesystem.emailservice.v1.SendEmailResponse;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.email.EmailType;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.utils.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class CtripPlatformEmailServiceTest {

    EmailService emailService;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        emailService = EmailService.DEFAULT;
    }

    @Test
    public void getOrder() throws Exception {
        int expected = -2147483648;
        Assert.assertEquals(expected, emailService.getOrder());
    }

    @Test
    public void sendEmail() throws IOException {
        emailService.sendEmail(generateEmail());
    }



    private Email generateEmail() throws IOException {
        String path = "src/test/resources/ctripPlatformEmailServiceTest.txt";
        InputStream ins = FileUtils.getFileInputStream(path);
        String text = IOUtils.toString(ins);
        Email email = new Email();
        email.setBodyContent(text);
        email.setEmailType(EmailType.CONSOLE_ALERT);
        email.addRecipient("zhuchen@ctrip.com");
        email.setSender("xpipe@test.com");
        email.setSubject("XPipe Test");
        return email;
    }

}