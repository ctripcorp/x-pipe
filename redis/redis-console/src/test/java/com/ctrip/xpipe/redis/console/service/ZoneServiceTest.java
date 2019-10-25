package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author taotaotu
 * May 24, 2019
 */
public class ZoneServiceTest extends AbstractConsoleIntegrationTest {

    private static final String H2DB_ZONE_SHA = "SHA";

    private static final String NEW_ZONE_NAME = "SFO";

    private static final long NEW_ZONE_ID = 3;

    @Autowired
    private ZoneService zoneService;

    @Test
    public void testQuery(){
        ZoneTbl zoneTbl = zoneService.findById(1);

        Assert.assertEquals(H2DB_ZONE_SHA, zoneTbl.getZoneName());
    }

    @Test
    public void testQueryAll(){
        List<ZoneTbl> zoneTbls = zoneService.findAllZones();

        Assert.assertEquals(2, zoneTbls.size());
        Assert.assertEquals(H2DB_ZONE_SHA, zoneTbls.get(0).getZoneName());
    }

    @Test
    public void testCreateZone(){
        zoneService.insertRecord(NEW_ZONE_NAME);
        ZoneTbl zoneTbl = zoneService.findById(NEW_ZONE_ID);

        Assert.assertEquals(NEW_ZONE_NAME, zoneTbl.getZoneName());
    }
}
