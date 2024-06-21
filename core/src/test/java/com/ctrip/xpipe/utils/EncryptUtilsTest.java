package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2024/6/20
 */
public class EncryptUtilsTest extends AbstractTest {

    @Test
    public void testAES_ECB_short() throws Exception {
        String content = "12345678";
        String secretKey = "87654321";

        String encryptedContent = EncryptUtils.encryptAES_ECB(content, secretKey);
        Assert.assertEquals("avpXiIl2sJeL06Xix1/utA==", encryptedContent);
        Assert.assertEquals(content, EncryptUtils.decryptAES_ECB(encryptedContent, secretKey));
    }

    @Test
    public void testAES_ECB_long() throws Exception {
        String content = "12345678123456781234567812345678";
        String secretKey = "87654321876543218765432187654321";

        String encryptedContent = EncryptUtils.encryptAES_ECB(content, secretKey);
        Assert.assertEquals("URuooij/UgWre7v45aRQnlEbqKIo/1IFq3u7+OWkUJ7PEj5VnOXH4bm9pcI0x7yO", encryptedContent);
        Assert.assertEquals(content, EncryptUtils.decryptAES_ECB(encryptedContent, secretKey));
    }

}
