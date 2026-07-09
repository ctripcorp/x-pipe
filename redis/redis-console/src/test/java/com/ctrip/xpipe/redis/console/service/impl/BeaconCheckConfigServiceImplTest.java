package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.core.beacon.BeaconConstant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Date;

@RunWith(MockitoJUnitRunner.class)
public class BeaconCheckConfigServiceImplTest {

    @Mock
    private DcClusterShardService dcClusterShardService;

    @InjectMocks
    private BeaconCheckConfigServiceImpl beaconCheckConfigService;

    @Before
    public void setup() throws Exception {
        Mockito.doReturn(1).when(dcClusterShardService).batchUpdateOperatingUntil(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyList(), Mockito.any(Date.class));
    }

    @Test
    public void testStopBeaconCheck() throws Exception {
        beaconCheckConfigService.stopBeaconCheck("cluster1", "jq",
                Collections.singletonList("shard1"), 30);
        Mockito.verify(dcClusterShardService).batchUpdateOperatingUntil(Mockito.eq("jq"), Mockito.eq("cluster1"),
                Mockito.eq(Collections.singletonList("shard1")), Mockito.argThat(date ->
                        date.getTime() > System.currentTimeMillis()));
    }

    @Test
    public void testStartBeaconCheck() throws Exception {
        beaconCheckConfigService.startBeaconCheck("cluster1", "jq", Collections.singletonList("shard1"));
        Mockito.verify(dcClusterShardService).batchUpdateOperatingUntil(Mockito.eq("jq"), Mockito.eq("cluster1"),
                Mockito.eq(Collections.singletonList("shard1")), Mockito.eq(BeaconConstant.DEFAULT_OPERATING_UNTIL));
    }
}
