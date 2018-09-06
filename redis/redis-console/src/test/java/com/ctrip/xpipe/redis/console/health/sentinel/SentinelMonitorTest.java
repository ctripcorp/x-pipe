package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.resources.DefaultMetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class SentinelMonitorTest extends AbstractConsoleIntegrationTest {
    @Autowired
    private DefaultSentinelCollector sentinelCollector;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private DefaultMetaCache metaCache;

    private SentinelSample sentinelSample;

    private SentinelSamplePlan sentinelSamplePlan;

    @BeforeClass
    public static void beforeSentinelMonitorTestClass(){
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_PRODUCTION);
    }
    @Before
    public void beforeSentinelMonitorTest(){
        MockitoAnnotations.initMocks(this);
        sentinelSamplePlan = new SentinelSamplePlan("cluster2", "shard1", consoleConfig, "oy");
        sentinelSample     = new SentinelSample(System.currentTimeMillis(), System.nanoTime(), sentinelSamplePlan, 5000);

        sentinelCollector = spy(sentinelCollector);
        metaCache = spy(metaCache);
    }

    @Override
    public String prepareDatas(){
        try {
            return prepareDatasFromFile("src/test/resources/sentinel-test.sql");
        }catch (Exception e){
            logger.error("prepare datas error for path", e);
        }
        return "";
    }

    @Test
    public void testSentinelAddBug() throws Exception{
        waitConditionUntilTimeOut(()->{if (sentinelCollector.getMetaCache().getXpipeMeta() != null)
                                            return true;
                                        else
                                            return false;}, 3000, 1000);
        sentinelCollector.collect(sentinelSample);
        verify(sentinelCollector, never()).checkAndDelete(any(), any(), any(), any());
        verify(sentinelCollector, never()).checkReset(any(), any(), any(), any());
        verify(sentinelCollector, never()).checkToAdd(any(), any(), any(), any(), any(), any(), any());

    }

}

