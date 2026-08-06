package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.client.RestClientException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogicalBuServiceWithoutDBTest {

    @Mock
    private ConsolePortalService consolePortalService;

    @Mock
    private ConsoleConfig config;

    @InjectMocks
    private LogicalBuServiceWithoutDB logicalBuService;

    @Before
    public void setUp() {
        when(config.getCacheRefreshInterval()).thenReturn(0);
        when(consolePortalService.getAllLogicalBus()).thenReturn(portalLogicalBus());
        logicalBuService.init();
    }

    @Test
    public void testFindAllFromPortal() {
        List<LogicalBuModel> all = logicalBuService.findAll();

        Assert.assertEquals(2, all.size());
        Assert.assertEquals(1L, all.get(0).getId());
        Assert.assertEquals("bu-a", all.get(0).getName());
        Assert.assertEquals("fs-b", all.get(1).getTfsFsId());
    }

    @Test
    public void testFindAllWhenPortalReturnsNullOnFirstLoad() {
        when(consolePortalService.getAllLogicalBus()).thenReturn(null);
        logicalBuService.init();

        Assert.assertTrue(logicalBuService.findAll().isEmpty());
    }

    @Test
    public void testFindAllKeepsLastValueWhenPortalReturnsNull() {
        Assert.assertEquals(2, logicalBuService.findAll().size());

        when(consolePortalService.getAllLogicalBus()).thenReturn(null);

        List<LogicalBuModel> all = logicalBuService.findAll();
        Assert.assertEquals(2, all.size());
        Assert.assertEquals("bu-a", all.get(0).getName());
    }

    @Test
    public void testFindAllKeepsLastValueWhenPortalThrows() {
        Assert.assertEquals(2, logicalBuService.findAll().size());

        when(consolePortalService.getAllLogicalBus())
                .thenThrow(new RestClientException("console unreachable"));

        List<LogicalBuModel> all = logicalBuService.findAll();
        Assert.assertEquals(2, all.size());
        Assert.assertEquals(1L, all.get(0).getId());
    }

    @Test
    public void testFindById() {
        LogicalBuModel model = logicalBuService.findById(2L);

        Assert.assertNotNull(model);
        Assert.assertEquals("bu-b", model.getName());
        Assert.assertEquals(Collections.singletonList(200L), model.getCmsOrgIds());
    }

    @Test(expected = BadRequestException.class)
    public void testFindByIdNotFound() {
        logicalBuService.findById(99L);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateUnsupported() {
        logicalBuService.create(new LogicalBuModel());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUpdateUnsupported() {
        logicalBuService.update(1L, new LogicalBuModel());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteUnsupported() {
        logicalBuService.delete(1L);
    }

    @Test
    public void testResolveLogicalBuIdForCluster() {
        // only bu-a is active and maps org 100; bu-b inactive so org 200 unbound
        Assert.assertEquals(1L, logicalBuService.resolveLogicalBuIdForCluster("cluster1", 100L));
        Assert.assertEquals(0L, logicalBuService.resolveLogicalBuIdForCluster("cluster1", 200L));
        Assert.assertEquals(0L, logicalBuService.resolveLogicalBuIdForCluster("cluster1", 0L));
    }

    private List<LogicalBuModel> portalLogicalBus() {
        return Arrays.asList(
                new LogicalBuModel().setId(1L).setName("bu-a").setTfsFsId("fs-a").setActive(true)
                        .setCmsOrgIds(Collections.singletonList(100L)),
                new LogicalBuModel().setId(2L).setName("bu-b").setTfsFsId("fs-b").setActive(false)
                        .setCmsOrgIds(Collections.singletonList(200L))
        );
    }
}
