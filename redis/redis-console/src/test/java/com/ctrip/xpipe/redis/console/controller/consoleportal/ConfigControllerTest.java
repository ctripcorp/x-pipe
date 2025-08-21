package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_ALERT_SYSTEM_ON;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
public class ConfigControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ConfigController controller;

    @Mock
    HttpServletRequest request;

    @Before
    public void beforeConfigControllerTest() {
        MockitoAnnotations.initMocks(this);
        when(request.getRequestURI()).thenReturn("localhost");
        when(request.getRemoteUser()).thenReturn("System");
    }

    @Test
    public void testChangeConfig1() throws Exception {
        ConfigModel model = new ConfigModel();
        model.setKey(KEY_ALERT_SYSTEM_ON);
        model.setVal(String.valueOf(false));
        RetMessage ret = controller.changeConfig(request, model);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());
    }

    @Test
    public void testChangeConfig2() throws Exception {
        ConfigModel model = new ConfigModel();
        model.setKey("Key Unknown");
        model.setVal(String.valueOf(false));
        RetMessage ret = controller.changeConfig(request, model);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertEquals("Unknown config key: Key Unknown", ret.getMessage());
    }

}