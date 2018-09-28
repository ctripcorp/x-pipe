package com.ctrip.xpipe.email;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class DefaultEmailServiceTest {

    EmailService emailService = EmailService.DEFAULT;

    @Test
    public void sendEmail() throws Exception {
        Email email = new Email();
        email.addRecipient("test@gmail.com");
        email.setSender("sender@gmail.com");
        email.setBodyContent("Hello world!");
        email.setSubject("Hello");
        emailService.sendEmail(email);
    }

    @Test
    public void getOrder() throws Exception {
        int expected = 2147483647;
        Assert.assertEquals(expected, emailService.getOrder());
    }

    @Test
    public void testClass() {
        Assert.assertTrue(emailService instanceof DefaultEmailService);
    }

}