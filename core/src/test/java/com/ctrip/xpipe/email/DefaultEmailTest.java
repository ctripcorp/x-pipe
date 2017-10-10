package com.ctrip.xpipe.email;

import com.ctrip.xpipe.api.email.Email;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class DefaultEmailTest {

    private Email email = Email.DEFAULT;

    private DefaultEmail defaultEmail;

    @Before
    public void before() {
        defaultEmail = (DefaultEmail) email;
    }

    @Test
    public void addRecipient() throws Exception {
        Assert.assertNotNull(email);
        defaultEmail.addRecipient("test@gmail.com");
    }

    @Test
    public void addCCer() throws Exception {
        defaultEmail.addCCer("cc@gmail.com");
    }

    @Test
    public void addBCCer() throws Exception {
        defaultEmail.addBCCer("bcc@gmail.com");
    }

    @Test
    public void testClass() {
        Assert.assertTrue(email instanceof DefaultEmail);
    }

}