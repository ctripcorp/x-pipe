package com.ctrip.xpipe.service.email;

import com.ctrip.xpipe.api.email.Email;
import com.ctrip.xpipe.api.email.EmailService;
import com.ctrip.xpipe.service.email.redis.alert.RedisAlertEmail;
import com.ctriposs.baiji.rpc.common.util.ServiceUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

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
    public void sendEmail() {
        List[] lists = new List[2];
        lists[0] = Arrays.asList("Cluster1,Shard1,10.3.2.23,6379",
                "Cluster2,Shard2,10.3.2.23,6380");
        lists[1] = Arrays.asList("Cluster2,Shard2,10.3.2.23,6379",
                "Cluster3,Shard3,10.3.2.23,6380",
                "Cluster4,Shard4,10.5.6.7,6479");
        Email email = new RedisAlertEmail();
        emailService.sendEmail(email, lists);
    }

}