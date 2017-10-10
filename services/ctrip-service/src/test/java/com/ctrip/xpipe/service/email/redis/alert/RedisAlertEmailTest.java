package com.ctrip.xpipe.service.email.redis.alert;

import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 10, 2017
 */
public class RedisAlertEmailTest {

    RedisAlertEmail email = new RedisAlertEmail();
    RedisAlertEmailConfig config = new RedisAlertEmailConfig();

    private static Logger logger = LoggerFactory.getLogger(RedisAlertEmailTest.class);


    @Test
    public void getRecipients() throws Exception {
        Assert.assertEquals(email.getEmailList(config.getDBAEmails()), email.getRecipients());
    }

    @Test
    public void getCCers() throws Exception {
        Assert.assertEquals(email.getEmailList(config.getCCEmails()), email.getCCers());
    }

    @Test
    public void getBCCers() throws Exception {
        Assert.assertEquals(null, email.getBCCers());
    }

    @Test
    public void getSender() throws Exception {
        Assert.assertEquals("XPipe@ctrip.com", email.getSender());
    }

    @Test
    public void getAppID() throws Exception {
        Assert.assertEquals(new Integer(100004374), email.getAppID());
    }

    @Test
    public void getBodyTemplateID() throws Exception {
        Assert.assertEquals(new Integer(37030053), email.getBodyTemplateID());
    }

    @Test
    public void isBodyHTML() throws Exception {
        Assert.assertEquals(true, email.isBodyHTML());
    }

    @Test
    public void getSendCode() throws Exception {
        Assert.assertEquals("37030053", email.getSendCode());
    }

    @Test
    public void getCharset() throws Exception {
        Assert.assertEquals("UTF-8", email.getCharset());
    }

    @Test
    public void getBodyContent() throws Exception {
        List[] lists = new List[2];
        lists[0] = Arrays.asList("Cluster1,Shard1,10.3.2.23,6379",
                "Cluster2,Shard2,10.3.2.23,6380");
        lists[1] = Arrays.asList("Cluster2,Shard2,10.3.2.23,6379",
                "Cluster3,Shard3,10.3.2.23,6380",
                "Cluster4,Shard4,10.5.6.7,6479");
        String expected = String.format("<entry>\n" +
                "    <environment><![CDATA[FWS]]></environment>\n" +
                "    <time><![CDATA[%s]]></time>\n" +
                "    <redisVersion><![CDATA[<tr><td>Cluster1</td><td>Shard1</td><td>10.3.2.23</td><td>6379</td></tr><tr><td>Cluster2</td><td>Shard2</td><td>10.3.2.23</td><td>6380</td></tr>]]></redisVersion>\n" +
                "    <redisConf><![CDATA[<tr><td>Cluster2</td><td>Shard2</td><td>10.3.2.23</td><td>6379</td></tr><tr><td>Cluster3</td><td>Shard3</td><td>10.3.2.23</td><td>6380</td></tr><tr><td>Cluster4</td><td>Shard4</td><td>10.5.6.7</td><td>6479</td></tr>]]></redisConf>\n" +
                "</entry>", DateTimeUtils.currentTimeAsString());
        logger.info("Expected:\n{}", expected);
        logger.info("Actual:\n{}", email.getBodyContent(lists));
    }

}