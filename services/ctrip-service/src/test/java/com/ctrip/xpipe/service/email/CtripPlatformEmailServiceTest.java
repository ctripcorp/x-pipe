package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.email.EmailType;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.utils.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class CtripPlatformEmailServiceTest {

    EmailService emailService;

    @Before
    public void before() {
        emailService = EmailService.DEFAULT;
    }

    @Test
    public void getOrder() throws Exception {
        int expected = -2147483648;
        Assert.assertEquals(expected, emailService.getOrder());
    }

    @Test
    public void sendEmail() throws IOException {
        String path = "src/test/resources/ctripPlatformEmailServiceTest.txt";
        InputStream ins = FileUtils.getFileInputStream(path);
        String text = IOUtils.toString(ins);
        Email email = new Email();
        email.setBodyContent(text);
        email.setEmailType(EmailType.CONSOLE_ALERT);
        email.addRecipient("zhuchen@ctrip.com");
        email.setSender("xpipe@test.com");
        email.setSubject("XPipe Test");
        emailService.sendEmail(email);
    }

}