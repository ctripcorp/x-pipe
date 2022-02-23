package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.SentinelInstanceModel;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SyncSentinels {

    private static final String url="http://xpipe.uat.qa.nt.ctripcorp.com/api/sentinels";

    public static int DEFAULT_RETRY_INTERVAL_MILLI = Integer
            .parseInt(System.getProperty("metaserver.retryIntervalMilli", "10"));
    protected static int DEFAULT_MAX_PER_ROUTE = Integer.parseInt(System.getProperty("max-per-route", "1000"));
    protected static int DEFAULT_MAX_TOTAL = Integer.parseInt(System.getProperty("max-per-route", "10000"));
    protected static int DEFAULT_RETRY_TIMES = Integer.parseInt(System.getProperty("retry-times", "3"));
    protected static int DEFAULT_CONNECT_TIMEOUT = Integer.parseInt(System.getProperty("connect-timeout", "1500"));
    protected static int DEFAULT_SO_TIMEOUT = Integer.parseInt(System.getProperty("so-timeout", "120000"));
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected RestOperations restTemplate;

    @Before
    public void setUp() throws Exception {
        this.restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(
                DEFAULT_MAX_PER_ROUTE,
                DEFAULT_MAX_TOTAL,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_SO_TIMEOUT,
                DEFAULT_RETRY_TIMES,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(DEFAULT_RETRY_INTERVAL_MILLI));
    }

    @Test
    public void syncSentinels() throws Exception {
        List<String> sentinelInfos = getContext();
        for (String sentinelInfo : sentinelInfos) {
            String sentinelInfoString[] = sentinelInfo.split("\\s");
            long sentinelGroupId = Long.parseLong(sentinelInfoString[0]);
            long dcId = Long.parseLong(sentinelInfoString[1]);
            String sentinelGroupAddress = sentinelInfoString[2];
//            String desc = sentinelInfoString[3];
            List<SentinelInstanceModel> groupSentinels = new ArrayList<>();
            String[] sentinelStrings = sentinelGroupAddress.split("\\s,\\s");
            for (String sentinelString : sentinelStrings) {
                String[] sentinelAddress = sentinelString.split(":");
                groupSentinels.add(new SentinelInstanceModel().setSentinelId(sentinelGroupId).setDcId(dcId).setSentinelIp(sentinelAddress[0]).setSentinelPort(Integer.parseInt(sentinelAddress[1])));
            }
            SentinelGroupModel sentinelGroupModel = new SentinelGroupModel().setSentinelGroupId(sentinelGroupId).setSentinels(groupSentinels);
            try {
                restTemplate.postForObject(url, sentinelGroupModel, RetMessage.class, new HashMap<>());
                logger.info("{} added", sentinelInfo);
            } catch (Exception e) {
                logger.error("add {} failed", sentinelInfo);
            }
            Thread.sleep(50);
        }
    }

    private static List<String> getContext() {
        List<String> context = new ArrayList<>();
        try {
            File file = new File(getConfigUrl().getPath());
            InputStreamReader inputReader = new InputStreamReader(new FileInputStream(file));
            BufferedReader bf = new BufferedReader(inputReader);
            String str;
            while ((str = bf.readLine()) != null) {
                context.add(str.trim());
            }
            bf.close();
            inputReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return context;
    }

    private static URL getConfigUrl() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null)
            classLoader = SyncSentinels.class.getClassLoader();

        URL dalconfigUrl = classLoader.getResource("sentinels");

        return dalconfigUrl;
    }

}
