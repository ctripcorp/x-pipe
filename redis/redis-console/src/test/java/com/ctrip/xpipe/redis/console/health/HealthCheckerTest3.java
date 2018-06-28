package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jun 20, 2018
 */
public class HealthCheckerTest3 {

    @Mock
    private ConsoleConfig consoleConfig;

    @InjectMocks
    private HealthChecker healthChecker = new HealthChecker();

    @Before
    public void beforeHealthCheckerTest3() {
        MockitoAnnotations.initMocks(this);
        when(consoleConfig.getIgnoredHealthCheckDc()).thenReturn(Sets.newHashSet("jq", "AWS-FRA"));
    }

    @Test
    public void testDcsToHealthCheck() {
        XpipeMeta meta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta().setId("jq");
        meta.addDc(dcMeta);

        List<DcMeta> result = healthChecker
                .dcsToCheck(meta);
        Assert.assertEquals(0, result.size());

        dcMeta = new DcMeta().setId("oy");
        meta.addDc(dcMeta);
        result = healthChecker
                .dcsToCheck(meta);
        Assert.assertEquals(1, result.size());

        dcMeta = new DcMeta().setId("AWS-FRA");
        meta.addDc(dcMeta);
        result = healthChecker
                .dcsToCheck(meta);
        Assert.assertEquals(1, result.size());
    }
}
