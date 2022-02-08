package com.ctrip.xpipe.service.foundation;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.service.AbstractServiceTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/3/2
 */
@RunWith(MockitoJUnitRunner.class)
public class CtripFoundationServiceTest extends AbstractServiceTest {

    @Mock
    private FoundationConfig config;

    private CtripFoundationService foundationService;

    @Before
    public void setupCtripFoundationServiceTest() {
        foundationService = new CtripFoundationService();
        CtripFoundationService.setConfig(config);
        foundationService = spy(foundationService);

        when(config.getGroupDcMap()).thenReturn(Collections.emptyMap());
    }

    @Test
    public void testGetDc() {
        System.setProperty(CtripFoundationService.DATA_CENTER_KEY, "jq");
        Assert.assertEquals("jq", foundationService.getDataCenter());
    }

    @Test
    public void testGroupDcMapping() {
        System.setProperty(CtripFoundationService.GROUP_ID_KEY, "123");
        System.setProperty(CtripFoundationService.DATA_CENTER_KEY, "jq");
        when(config.getGroupDcMap()).thenReturn(Collections.singletonMap("123", "oy"));
        Assert.assertEquals("oy", foundationService.getDataCenter());
    }

}
