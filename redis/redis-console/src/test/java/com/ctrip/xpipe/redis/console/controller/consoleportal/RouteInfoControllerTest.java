package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class RouteInfoControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    RouteInfoController controller;


    @Test
    public void testUpdateRoutes() {
        RouteInfoModel routeInfoModel1  = new RouteInfoModel().setActive(true).setPublic(false).setDstDcName(dcNames[0])
                .setSrcDcName(dcNames[1]).setId(4).setTag(Route.TAG_META)
                .setSrcProxies(Lists.newArrayList("PROXYTCP://127.0.0.1:80"))
                .setDstProxies(Lists.newArrayList("PROXYTLS://127.0.0.1:443"));

        RouteInfoModel routeInfoModel2  = new RouteInfoModel().setActive(true).setPublic(false).setDstDcName(dcNames[0])
                .setSrcDcName(dcNames[1]).setId(4).setTag(Route.TAG_META)
                .setSrcProxies(Lists.newArrayList("PROXYTCP://127.0.0.2:80"))
                .setDstProxies(Lists.newArrayList("PROXYTLS://127.0.0.2:443"));

        RetMessage retMessage = controller.updateRoutes(Lists.newArrayList(routeInfoModel1, routeInfoModel2));

        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage.getState());
        Assert.assertEquals("none public route in this direction", retMessage.getMessage());

    }

}