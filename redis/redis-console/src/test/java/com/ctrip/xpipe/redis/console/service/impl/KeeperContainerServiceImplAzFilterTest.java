package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.service.AzService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerServiceImplAzFilterTest {

    private static final String DC = "PTOY";

    @InjectMocks
    private KeeperContainerServiceImpl keeperContainerService;

    @Mock
    private AzService azService;

    @Test
    public void testSkipAzIdZeroAndKeepOnePerAz() {
        when(azService.getDcActiveAvailableZoneTbls(DC)).thenReturn(Arrays.asList(
                az(1L), az(2L)));

        List<KeepercontainerTbl> input = Arrays.asList(
                kc("10.0.0.1", 0L),
                kc("10.0.0.2", 1L),
                kc("10.0.0.3", 1L),
                kc("10.0.0.4", 2L));

        List<KeepercontainerTbl> result = keeperContainerService.filterKeeperContainersByAz(input, DC);

        Assert.assertEquals(2, result.size());
        Assert.assertEquals(
                Arrays.asList("10.0.0.2", "10.0.0.4"),
                result.stream().map(KeepercontainerTbl::getKeepercontainerIp).collect(Collectors.toList()));
    }

    @Test
    public void testDegradeWhenNoValidAzId() {
        when(azService.getDcActiveAvailableZoneTbls(DC)).thenReturn(Arrays.asList(
                az(1L), az(2L)));

        List<KeepercontainerTbl> input = Arrays.asList(
                kc("10.0.0.1", 0L),
                kc("10.0.0.2", 0L),
                kc("10.0.0.3", 99L));

        List<KeepercontainerTbl> result = keeperContainerService.filterKeeperContainersByAz(input, DC);

        Assert.assertSame(input, result);
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void testSingleAzNoFilter() {
        when(azService.getDcActiveAvailableZoneTbls(DC)).thenReturn(Arrays.asList(az(1L)));

        List<KeepercontainerTbl> input = Arrays.asList(
                kc("10.0.0.1", 0L),
                kc("10.0.0.2", 1L));

        List<KeepercontainerTbl> result = keeperContainerService.filterKeeperContainersByAz(input, DC);

        Assert.assertSame(input, result);
    }

    private static AzTbl az(long id) {
        return new AzTbl().setId(id).setActive(true);
    }

    private static KeepercontainerTbl kc(String ip, long azId) {
        return new KeepercontainerTbl()
                .setKeepercontainerIp(ip)
                .setKeepercontainerPort(8080)
                .setAzId(azId);
    }
}
