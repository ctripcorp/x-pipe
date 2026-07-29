package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.when;

public class RegionCacheImplTest {

    @Mock
    private DcCache dcCache;

    @Mock
    private ZoneService zoneService;

    @Mock
    private ConsoleConfig config;

    private RegionCacheImpl regionCache;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(config.getCacheRefreshInterval()).thenReturn(60_000);
        regionCache = new RegionCacheImpl(dcCache, zoneService, config);
    }

    @Test
    public void testFindByZoneId() {
        ZoneTbl sha = new ZoneTbl().setId(1L).setZoneName("SHA");
        when(zoneService.findById(1L)).thenReturn(sha);

        Assert.assertEquals("SHA", regionCache.find(1L).getZoneName());
        Assert.assertNull(regionCache.find(99L));
    }

    @Test
    public void testFindByZoneName() {
        ZoneTbl sha = new ZoneTbl().setId(1L).setZoneName("SHA");
        ZoneTbl fra = new ZoneTbl().setId(2L).setZoneName("FRA");
        when(zoneService.findAllZones()).thenReturn(Arrays.asList(sha, fra));

        Assert.assertEquals(1L, (long) regionCache.find("sha").getId());
        Assert.assertEquals(2L, (long) regionCache.find("FRA").getId());
        Assert.assertNull(regionCache.find("SGP"));
        Assert.assertNull(regionCache.find(null));
    }

    @Test
    public void testRegionOfDcNameAndId() {
        ZoneTbl sha = new ZoneTbl().setId(1L).setZoneName("SHA");
        when(zoneService.findById(1L)).thenReturn(sha);
        when(dcCache.find("jq")).thenReturn(new DcTbl().setId(1L).setDcName("jq").setZoneId(1L));
        when(dcCache.find(1L)).thenReturn(new DcTbl().setId(1L).setDcName("jq").setZoneId(1L));
        when(dcCache.find("missing")).thenReturn(null);

        Assert.assertEquals("SHA", regionCache.regionOf("jq"));
        Assert.assertEquals("SHA", regionCache.regionOf(1L));
        Assert.assertEquals("", regionCache.regionOf("missing"));
        Assert.assertEquals("", regionCache.regionOf(""));
        Assert.assertEquals("", regionCache.regionOf(0L));
    }

    @Test
    public void testRegionOfWhenZoneMissing() {
        when(dcCache.find(2L)).thenReturn(new DcTbl().setId(2L).setDcName("oy").setZoneId(9L));
        when(zoneService.findById(9L)).thenReturn(null);

        Assert.assertEquals("", regionCache.regionOf(2L));
    }

    @Test
    public void testFindByZoneNameEmptyList() {
        when(zoneService.findAllZones()).thenReturn(Collections.emptyList());
        Assert.assertNull(regionCache.find("SHA"));
    }
}
