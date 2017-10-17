package com.ctrip.xpipe.redis.console.util;

import com.ctrip.xpipe.api.email.EMAIL_TYPE;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 17, 2017
 */
public class EmailUtilTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private EmailUtil emailUtil;

    @Test
    public void fillRecipientsAndCCersByType() throws Exception {
        Email email = new Email();
        email.setEmailType(EMAIL_TYPE.SEND_TO_DBA_CC_DEV);
        emailUtil.fillRecipientsAndCCersByType(email);
        logger.info("{}", email.getRecipients());
        logger.info("{}", email.getCCers());
        Assert.assertEquals(emailUtil.getDBAEmails(), email.getRecipients());
        Assert.assertEquals(emailUtil.getXPipeAdminEmails(), email.getCCers());
    }

    @Test
    public void getDBAEmails() throws Exception {
        List<String> emails = emailUtil.getDBAEmails();
        logger.info("{}", emails);
    }

    @Test
    public void getXPipeAdminEmails() throws Exception {
        List<String> emails = emailUtil.getXPipeAdminEmails();
        logger.info("{}", emails);
    }

}