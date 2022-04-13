package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ProxyTbl;
import com.ctrip.xpipe.redis.console.model.RouteTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Jun 20, 2018
 */
public class AdvancedDcMetaServiceTestForRoute extends AbstractConsoleIntegrationTest {

    @Autowired
    private AdvancedDcMetaService service;

    @Autowired
    private DcService dcService;

    @Autowired
    private ProxyService proxyService;

    private Map<Long, ProxyTbl> proxyTblMap = Maps.newHashMap();

    private String PROXY1 = "PROXYTCP://10.2.1.1:80";
    private String PROXY2 = "PROXYTCP://10.2.1.2:80";
    private String PROXY3 = "PROXYTCP://10.2.1.3:80";

    private String PROXY4 = "PROXYTLS://10.3.1.1:443";
    private String PROXY5 = "PROXYTLS://10.3.1.2:443";
    private String PROXY6 = "PROXYTLS://10.3.1.3:443";

    private String PROXY7 = "PROXYTLS://135.1.1.1:443";
    private String PROXY8 = "PROXYTLS://135.1.1.2:443";
    private String PROXY9 = "PROXYTLS://135.1.1.3:443";


    @Before
    public void beforeAdvancedDcMetaServiceTestForRoute() {
        proxyTblMap.put(1L, new ProxyTbl().setId(1L).setActive(true).setDcId(1L).setUri(PROXY1));
        proxyTblMap.put(2L, new ProxyTbl().setId(2L).setActive(true).setDcId(1L).setUri(PROXY2));
        proxyTblMap.put(3L, new ProxyTbl().setId(3L).setActive(true).setDcId(1L).setUri(PROXY3));
        proxyTblMap.put(4L, new ProxyTbl().setId(4L).setActive(true).setDcId(2L).setUri(PROXY4));
        proxyTblMap.put(5L, new ProxyTbl().setId(5L).setActive(true).setDcId(2L).setUri(PROXY5));
        proxyTblMap.put(6L, new ProxyTbl().setId(6L).setActive(true).setDcId(2L).setUri(PROXY6));
        proxyTblMap.put(7L, new ProxyTbl().setId(7L).setActive(true).setDcId(3L).setUri(PROXY7));
        proxyTblMap.put(8L, new ProxyTbl().setId(8L).setActive(true).setDcId(3L).setUri(PROXY8));
        proxyTblMap.put(9L, new ProxyTbl().setId(9L).setActive(true).setDcId(3L).setUri(PROXY9));
    }

    @Test
    public void testFetchRouteInfo() {
        StringBuilder sb = new StringBuilder();
        service.fetchRouteInfo("1,2,3", proxyTblMap, sb);
        Assert.assertEquals(PROXY1+","+PROXY2+","+PROXY3+" ", sb.toString());
        logger.info("[output] {}", sb.toString());

        sb = new StringBuilder();
        service.fetchRouteInfo("", proxyTblMap, sb);
        Assert.assertEquals("", sb.toString());
    }

    @Test
    public void testFetchRouteInfo2() {
        StringBuilder sb = new StringBuilder();
        service.fetchRouteInfo("2,3", proxyTblMap, sb);
        Assert.assertEquals(PROXY2+","+PROXY3+" ", sb.toString());
        logger.info("[output] {}", sb.toString());

        service.fetchRouteInfo("", proxyTblMap, sb);
        Assert.assertEquals(PROXY2+","+PROXY3+" ", sb.toString());
    }

    @Test
    public void testGetRouteInfo() {

        RouteTbl route = new RouteTbl().setSrcProxyIds("1,2").setDstProxyIds("4,5,6").setOptionalProxyIds("");
        String routeInfo = service.getRouteInfo(route, proxyTblMap);

        Assert.assertEquals(PROXY1+","+PROXY2+" "+PROXY4+","+PROXY5+","+PROXY6, routeInfo);
        logger.info("[route-info] {}", routeInfo);
    }

    @Test
    public void testGetRouteInfo2() {
        RouteTbl route = new RouteTbl().setSrcProxyIds("1,2").setDstProxyIds("4,5,6").setOptionalProxyIds("3");
        String routeInfo = service.getRouteInfo(route, proxyTblMap);

        Assert.assertEquals(PROXY1+","+PROXY2+" "+PROXY3+" "+PROXY4+","+PROXY5+","+PROXY6, routeInfo);
        logger.info("[route-info] {}", routeInfo);
    }

    @Test
    public void testCombineRouteInfo() {
        List<RouteTbl> routes = Lists.newArrayList();
        routes.add(new RouteTbl().setSrcProxyIds("1,2").setDstProxyIds("4,5,6").setOptionalProxyIds("3")
                .setSrcDcId(1L).setDstDcId(3L).setTag("console").setIsPublic(true));
        List<ProxyTbl> proxies = Lists.newArrayList();
        proxies.addAll(proxyTblMap.values());
        List<RouteMeta> routeMetas = service.combineRouteInfo(routes, proxies, new DcMeta().setId("jq"));
        logger.info("{}", routeMetas.get(0));
        Assert.assertEquals(PROXY1+","+PROXY2+" "+PROXY3+" "+PROXY4+","+PROXY5+","+PROXY6, routeMetas.get(0).getRouteInfo());
        Assert.assertEquals(true, routeMetas.get(0).getIsPublic());
    }
}
