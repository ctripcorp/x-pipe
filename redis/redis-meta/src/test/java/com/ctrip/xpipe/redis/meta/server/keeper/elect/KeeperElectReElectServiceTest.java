package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KeeperElectReElectServiceTest {

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager;

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private KeeperActiveElectAlgorithm electAlgorithm;

    @InjectMocks
    private KeeperElectReElectService keeperElectReElectService;

    private KeeperMeta activeKeeper;
    private KeeperMeta backupKeeper;

    @Before
    public void setUp() {
        activeKeeper = keeper(6000, 1);
        backupKeeper = keeper(6001, 2);
    }

    @Test
    public void testReElectCallsSetSurviveKeepers() {
        List<KeeperMeta> surviveKeepers = Arrays.asList(activeKeeper, backupKeeper);
        when(currentMetaManager.getSurviveKeepers(1L, 2L)).thenReturn(surviveKeepers);
        when(dcMetaCache.getShardKeepers(1L, 2L)).thenReturn(surviveKeepers);
        when(keeperActiveElectAlgorithmManager.get(1L, 2L)).thenReturn(electAlgorithm);
        when(electAlgorithm.select(eq(1L), eq(2L), anyList())).thenReturn(activeKeeper);

        keeperElectReElectService.reElect(1L, 2L);

        verify(currentMetaManager).setSurviveKeepers(1L, 2L, surviveKeepers, activeKeeper);
    }

    @Test
    public void testReElectNoOpWhenNoSurvivors() {
        when(currentMetaManager.getSurviveKeepers(1L, 2L)).thenReturn(Collections.emptyList());

        keeperElectReElectService.reElect(1L, 2L);

        verify(currentMetaManager, never()).setSurviveKeepers(anyLong(), anyLong(), anyList(), any());
    }

    private KeeperMeta keeper(int port, int priority) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp("127.0.0.1");
        keeperMeta.setPort(port);
        keeperMeta.setPriority(priority);
        return keeperMeta;
    }
}
