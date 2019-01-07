package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class CtripPlatformEmailServiceTest {

    private static Logger logger = LoggerFactory.getLogger(CtripPlatformEmailServiceTest.class);

    CtripPlatformEmailService emailService;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        emailService = new CtripPlatformEmailService();
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


    @Test
    public void testCheckAsyncEmailResult() throws Exception {
        CommandFuture<EmailResponse> future = emailService.sendEmailAsync(generateEmail());
        EmailResponse response = future.get();
        logger.info("response: {}", response.getProperties());

        String encode = Codec.DEFAULT.encode(response.getProperties());
        EmailResponse newResponse = new EmailResponse() {
            @Override
            public Properties getProperties() {
                return Codec.DEFAULT.decode(encode, Properties.class);
            }
        };

        logger.info("new Response: {}", newResponse.getProperties());
        emailService.checkAsyncEmailResult(newResponse);
    }

    @Test
    public void testEncodeListString() {
        String expected = "123,456,789";
        String result = CtripPlatformEmailService.encodeListString(Lists.newArrayList("123", "456", "789"));
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testDecodeListString() {
        List<String> expected = Lists.newArrayList("123", "456", "789");
        List<String> result = CtripPlatformEmailService.decodeListString("123,456,789");
        Assert.assertEquals(expected, result);
    }

    private Email generateEmail() throws IOException {
        String path = "src/test/resources/ctripPlatformEmailServiceTest.txt";
        InputStream ins = FileUtils.getFileInputStream(path);
        String text = IOUtils.toString(ins);
        Email email = new Email();
        email.setBodyContent(text);
        email.addRecipient("zhuchen@ctrip.com");
        email.addRecipient("tt.tu@ctrip.com");
        email.setSender("xpipe@test.com");
        email.setSubject("XPipe Test");
        return email;
    }

}