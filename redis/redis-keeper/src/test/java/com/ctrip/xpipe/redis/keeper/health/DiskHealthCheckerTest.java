package com.ctrip.xpipe.redis.keeper.health;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * @author lishanglin
 * date 2023/12/12
 */
@RunWith(MockitoJUnitRunner.class)
public class DiskHealthCheckerTest extends AbstractRedisKeeperTest {
    @Mock
    private KeeperContainerConfig keeperContainerConfig;

    private DiskHealthChecker checker;

    private int checkRound = 2;

    @Before
    public void setupDiskHealthCheckerTest() {
        this.checker = new DiskHealthChecker(keeperContainerConfig);
        Mockito.when(keeperContainerConfig.checkRoundBeforeMarkDown()).thenReturn(checkRound);
    }

    @Test
    public void testDiskDown() {
        List<HealthState> checkResult = new ArrayList<>();
        List<HealthState> expect = new ArrayList<>();
        checker.addObserver(new Observer() {
            @Override
            public void update(Object args, Observable observable) {
                checkResult.add((HealthState) args);
            }
        });

        KeeperDiskInfo diskInfo = new KeeperDiskInfo();
        diskInfo.available = true;
        checker.setResult(diskInfo);
        expect.add(HealthState.HEALTHY);

        diskInfo.available = false;
        IntStream.range(0, checkRound - 1).forEach(i -> {
            checker.setResult(diskInfo);
            expect.add(HealthState.SICK);
        });

        checker.setResult(diskInfo);
        expect.add(HealthState.DOWN);

        Assert.assertEquals(expect, checkResult);
    }

    @Test
    public void testDiskSickRecover() {
        Assert.assertEquals(HealthState.HEALTHY, checker.getState());

        KeeperDiskInfo diskInfo = new KeeperDiskInfo();
        diskInfo.available = false;
        checker.setResult(diskInfo);
        Assert.assertEquals(HealthState.SICK, checker.getState());

        diskInfo.available = true;
        checker.setResult(diskInfo);
        Assert.assertEquals(HealthState.HEALTHY, checker.getState());
    }

    @Test
    public void testDiskDownRecover() {
        Assert.assertEquals(HealthState.HEALTHY, checker.getState());

        KeeperDiskInfo diskInfo = new KeeperDiskInfo();
        diskInfo.available = false;
        IntStream.range(0, checkRound).forEach(i -> checker.setResult(diskInfo));
        Assert.assertEquals(HealthState.DOWN, checker.getState());

        diskInfo.available = true;
        checker.setResult(diskInfo);
        Assert.assertEquals(HealthState.HEALTHY, checker.getState());
    }

}
