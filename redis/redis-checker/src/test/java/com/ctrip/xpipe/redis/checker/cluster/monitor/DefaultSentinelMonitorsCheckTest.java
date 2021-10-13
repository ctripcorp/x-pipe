package com.ctrip.xpipe.redis.checker.cluster.monitor;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.cluster.allleader.SentinelMonitorsCheckCrossDc;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class DefaultSentinelMonitorsCheckTest {

    @InjectMocks
    private SentinelMonitorsCheckCrossDc checker;

    @Mock
    private AlertManager alertManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private SentinelManager sentinelManager;

    @Mock
    private PersistenceCache persistenceCache;

    @Mock
    private  CheckerConfig config;

    @Before
    public void beforeDefaultSentinelMonitorsCheckTest() {
        checker = new SentinelMonitorsCheckCrossDc(metaCache, persistenceCache, config, FoundationService.DEFAULT.getDataCenter(), sentinelManager, alertManager);
        MockitoAnnotations.initMocks(this);
        String result = "sentinel_masters:82\n" +
                "sentinel_tilt:0\n" +
                "sentinel_running_scripts:0\n" +
                "sentinel_scripts_queue_length:0\n" +
                "master0:name=xpipe-auto-build-69-shard-2,status=ok,address=10.5.109.151:6447,slaves=2,sentinels=5\n" +
                "master1:name=xpipe-auto-build-48-shard-2,status=ok,address=10.5.109.151:6426,slaves=2,sentinels=5\n" +
                "master2:name=xpipe-auto-build-84-shard-3,status=ok,address=10.5.109.146:6462,slaves=2,sentinels=5\n" +
                "master3:name=xpipe-auto-build-79-shard-2,status=ok,address=10.5.109.151:6457,slaves=2,sentinels=5\n" +
                "master4:name=xpipe-auto-build-88-shard-1,status=ok,address=10.5.109.155:6466,slaves=2,sentinels=5\n" +
                "master5:name=xpipe-auto-build-92-shard-1,status=ok,address=10.5.109.154:6470,slaves=2,sentinels=5\n" +
                "master6:name=xpipe-auto-build-79-shard-3,status=ok,address=10.5.109.146:6457,slaves=2,sentinels=5\n" +
                "master7:name=xpipe_function-shard3,status=ok,address=10.2.55.173:6579,slaves=2,sentinels=5\n" +
                "master8:name=xpipe-auto-build-50-shard-3,status=ok,address=10.5.109.146:6428,slaves=2,sentinels=5\n" +
                "master9:name=xpipe-auto-build-92-shard-3,status=ok,address=10.5.109.146:6470,slaves=2,sentinels=5\n" +
                "master10:name=xpipe-auto-build-4-shard-3,status=ok,address=10.5.109.146:6382,slaves=2,sentinels=5\n" +
                "master11:name=xpipe-auto-build-75-shard-1,status=ok,address=10.5.109.155:6453,slaves=2,sentinels=5\n" +
                "master12:name=xpipe-auto-build-4-shard-2,status=ok,address=10.5.109.151:6382,slaves=3,sentinels=5\n" +
                "master13:name=xpipe-auto-build-78-shard-2,status=ok,address=10.5.109.150:6456,slaves=2,sentinels=5\n" +
                "master14:name=xpipe-auto-build-85-shard-2,status=ok,address=10.5.109.151:6463,slaves=2,sentinels=5\n" +
                "master15:name=xpipe_function-shard1,status=ok,address=10.2.25.214:6379,slaves=3,sentinels=5\n" +
                "master16:name=xpipe-auto-build-62-shard-1,status=ok,address=10.5.109.154:6440,slaves=2,sentinels=5\n" +
                "master17:name=xpipe-auto-build-87-shard-1,status=ok,address=10.5.109.155:6465,slaves=2,sentinels=5\n" +
                "master18:name=xpipe-auto-build-87-shard-3,status=ok,address=10.5.109.146:6465,slaves=2,sentinels=5\n" +
                "master19:name=xpipe-auto-build-50-shard-1,status=ok,address=10.5.109.154:6428,slaves=2,sentinels=5\n" +
                "master20:name=xpipe-auto-build-62-shard-2,status=ok,address=10.5.109.151:6440,slaves=2,sentinels=5\n" +
                "master21:name=xpipe-auto-build-85-shard-3,status=ok,address=10.5.109.147:6463,slaves=2,sentinels=5\n" +
                "master22:name=xpipe-auto-build-63-shard-3,status=ok,address=10.5.109.146:6441,slaves=2,sentinels=5\n" +
                "master23:name=xpipe-auto-build-78-shard-1,status=ok,address=10.5.109.154:6456,slaves=2,sentinels=5\n" +
                "master24:name=xpipe-auto-build-82-shard-1,status=ok,address=10.5.109.154:6460,slaves=2,sentinels=5\n" +
                "master25:name=xpipe-auto-build-84-shard-1,status=ok,address=10.5.109.154:6462,slaves=2,sentinels=5\n" +
                "master26:name=xpipe-auto-build-62-shard-3,status=ok,address=10.5.109.146:6440,slaves=2,sentinels=5\n" +
                "master27:name=xpipe-auto-build-75-shard-3,status=ok,address=10.5.109.147:6453,slaves=2,sentinels=5\n" +
                "master28:name=xpipe-auto-build-88-shard-2,status=ok,address=10.5.109.151:6466,slaves=2,sentinels=5\n" +
                "master29:name=xpipe-auto-build-56-shard-2,status=ok,address=10.5.109.150:6434,slaves=2,sentinels=5\n" +
                "master30:name=xpipe-auto-build-66-shard-2,status=ok,address=10.5.109.150:6444,slaves=2,sentinels=5\n" +
                "master31:name=xpipe-auto-build-39-shard-1,status=ok,address=10.5.109.155:6417,slaves=2,sentinels=5\n" +
                "master32:name=xpipe-auto-build-88-shard-3,status=ok,address=10.5.109.146:6466,slaves=2,sentinels=5\n" +
                "master33:name=xpipe-auto-build-41-shard-2,status=ok,address=10.5.109.151:6419,slaves=2,sentinels=5\n" +
                "master34:name=xpipe-auto-build-57-shard-2,status=ok,address=10.5.109.151:6435,slaves=2,sentinels=5\n" +
                "master35:name=xpipe-auto-build-65-shard-1,status=ok,address=10.5.109.155:6443,slaves=2,sentinels=5\n" +
                "master36:name=xpipe-auto-build-41-shard-3,status=ok,address=10.5.109.147:6419,slaves=2,sentinels=5\n" +
                "master37:name=xpipe-auto-build-66-shard-3,status=ok,address=10.5.109.146:6444,slaves=2,sentinels=5\n" +
                "master38:name=xpipe-auto-build-66-shard-1,status=ok,address=10.5.109.154:6444,slaves=2,sentinels=5\n" +
                "master39:name=xpipe-auto-build-92-shard-2,status=ok,address=10.5.109.150:6470,slaves=2,sentinels=5\n" +
                "master40:name=xpipe-auto-build-56-shard-1,status=ok,address=10.5.109.154:6434,slaves=2,sentinels=5\n" +
                "master41:name=xpipe-auto-build-65-shard-2,status=ok,address=10.5.109.151:6443,slaves=2,sentinels=5\n" +
                "master42:name=xpipe-auto-build-85-shard-1,status=ok,address=10.5.109.155:6463,slaves=2,sentinels=5\n" +
                "master43:name=xpipe-auto-build-63-shard-1,status=ok,address=10.5.109.155:6441,slaves=2,sentinels=5\n" +
                "master44:name=xpipe-auto-build-82-shard-2,status=ok,address=10.5.109.150:6460,slaves=2,sentinels=5\n" +
                "master45:name=xpipe_function-shard2,status=ok,address=10.2.55.173:6479,slaves=2,sentinels=5\n" +
                "master46:name=xpipe_UATxpipe2_uatgroup0,status=ok,address=10.2.106.64:6379,slaves=2,sentinels=5\n" +
                "master47:name=xpipe-auto-build-39-shard-3,status=ok,address=10.5.109.147:6417,slaves=2,sentinels=5\n" +
                "master48:name=xpipe-auto-build-59-shard-3,status=ok,address=10.5.109.146:6437,slaves=2,sentinels=5\n" +
                "master49:name=xpipe-auto-build-65-shard-3,status=ok,address=10.5.109.146:6443,slaves=2,sentinels=5\n" +
                "master50:name=xpipe-auto-build-90-shard-3,status=ok,address=10.5.109.147:6468,slaves=2,sentinels=5\n" +
                "master51:name=xpipe-auto-build-49-shard-2,status=ok,address=10.5.109.151:6427,slaves=2,sentinels=5\n" +
                "master52:name=xpipe-auto-build-84-shard-2,status=ok,address=10.5.109.150:6462,slaves=2,sentinels=5\n" +
                "master53:name=xpipe-auto-build-59-shard-2,status=ok,address=10.5.109.151:6437,slaves=2,sentinels=5\n" +
                "master54:name=xpipe_functionshard5,status=ok,address=10.2.55.173:6779,slaves=2,sentinels=5\n" +
                "master55:name=xpipe_version1shard1,status=ok,address=10.2.25.214:7379,slaves=2,sentinels=5\n" +
                "master56:name=xpipe-auto-build-69-shard-3,status=ok,address=10.5.109.147:6447,slaves=2,sentinels=5\n" +
                "master57:name=xpipe-auto-build-63-shard-2,status=ok,address=10.5.109.151:6441,slaves=2,sentinels=5\n" +
                "master58:name=xpipe-auto-build-78-shard-3,status=ok,address=10.5.109.146:6456,slaves=2,sentinels=5\n" +
                "master59:name=xpipe-auto-build-50-shard-2,status=ok,address=10.5.109.151:6428,slaves=2,sentinels=5\n" +
                "master60:name=xpipe-auto-build-79-shard-1,status=ok,address=10.5.109.155:6457,slaves=2,sentinels=5\n" +
                "master61:name=xpipe-auto-build-39-shard-2,status=ok,address=10.5.109.151:6417,slaves=2,sentinels=5\n" +
                "master62:name=xpipe-auto-build-56-shard-3,status=ok,address=10.5.109.146:6434,slaves=2,sentinels=5\n" +
                "master63:name=xpipe-auto-build-75-shard-2,status=ok,address=10.5.109.151:6453,slaves=2,sentinels=5\n" +
                "master64:name=xpipe-auto-build-41-shard-1,status=ok,address=10.5.109.155:6419,slaves=2,sentinels=5\n" +
                "master65:name=xpipe-auto-build-69-shard-1,status=ok,address=10.5.109.155:6447,slaves=2,sentinels=5\n" +
                "master66:name=xpipe_UATxpipe2_uatgroup1,status=ok,address=10.2.107.125:6381,slaves=2,sentinels=5\n" +
                "master67:name=xpipe-auto-build-57-shard-1,status=ok,address=10.5.109.154:6435,slaves=2,sentinels=5\n" +
                "master68:name=xpipe-auto-build-90-shard-1,status=ok,address=10.5.109.155:6468,slaves=2,sentinels=5\n" +
                "master69:name=xpipe-auto-build-90-shard-2,status=ok,address=10.5.109.151:6468,slaves=2,sentinels=5\n" +
                "master70:name=xpipe-auto-build-4-shard-1,status=ok,address=10.5.109.154:6382,slaves=2,sentinels=5\n" +
                "master71:name=xpipe-auto-build-82-shard-3,status=ok,address=10.5.109.146:6460,slaves=2,sentinels=5\n" +
                "master72:name=xpipe-auto-build-48-shard-1,status=ok,address=10.5.109.154:6426,slaves=2,sentinels=5\n" +
                "master73:name=xpipe_version2shard1,status=ok,address=10.2.25.215:7379,slaves=2,sentinels=5\n" +
                "master74:name=xpipe_function-shard4,status=ok,address=10.2.55.173:6679,slaves=2,sentinels=5\n" +
                "master75:name=xpipe-auto-build-49-shard-1,status=ok,address=10.5.109.154:6427,slaves=2,sentinels=5\n" +
                "master76:name=xpipe-auto-build-57-shard-3,status=ok,address=10.5.109.147:6435,slaves=2,sentinels=5\n" +
                "master77:name=xpipe-auto-build-48-shard-3,status=ok,address=10.5.109.146:6426,slaves=2,sentinels=5\n" +
                "master78:name=xpipe_UATGroup0,status=ok,address=10.2.106.64:6380,slaves=2,sentinels=5\n" +
                "master79:name=xpipe-auto-build-59-shard-1,status=ok,address=10.5.109.155:6437,slaves=2,sentinels=5\n" +
                "master80:name=xpipe-auto-build-49-shard-3,status=ok,address=10.5.109.147:6427,slaves=2,sentinels=5\n" +
                "master81:name=xpipe-auto-build-87-shard-2,status=ok,address=10.5.109.151:6465,slaves=2,sentinels=5";
        when(sentinelManager.infoSentinel(any())).thenReturn(result);
        when(alertManager.shouldAlert(any())).thenReturn(true);
        when(persistenceCache.isSentinelAutoProcess()).thenReturn(true);
    }

    @Test
    public void checkSentinel() throws Exception {
        when(metaCache.findClusterShardBySentinelMonitor(any())).thenReturn(null);
        checker.checkSentinel(new SentinelMeta().setAddress("127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002"),
                new HostPort("127.0.0.1", 5000));
        verify(alertManager, atLeastOnce()).alert(eq(null), eq(null), eq(null), eq(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS), anyString());
        verify(sentinelManager, atLeastOnce()).removeSentinelMonitor(any(), any());
    }

    @Test
    public void checkSentinel2() throws Exception {
        when(metaCache.findClusterShardBySentinelMonitor(any())).thenReturn(new Pair<>("cluster", "shard"));
        checker.setMetaCache(metaCache);
        checker.checkSentinel(new SentinelMeta().setAddress("127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002"),
                new HostPort("127.0.0.1", 5000));
        verify(alertManager, never()).alert(eq(null), eq(null), eq(null), eq(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
    }

}
