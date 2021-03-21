package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.utils.OsUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 * Mar 18, 2021
 */
public class DefaultReplicationStoreStatsTest extends AbstractTest {

    @Test
    public void test() {

        long exptected = System.currentTimeMillis() - OsUtils.APPROXIMATE__RESTART_TIME_MILLI;
        DefaultReplicationStoreStats replicationStoreStats = new DefaultReplicationStoreStats();

        long currentValue = replicationStoreStats.getLastReplDownTime();
        Assert.assertTrue(currentValue >= (exptected - 50)  && currentValue <= (exptected + 50));


        replicationStoreStats.setMasterState(MASTER_STATE.REDIS_REPL_HANDSHAKE);
        Assert.assertEquals(currentValue, replicationStoreStats.getLastReplDownTime());

        replicationStoreStats.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        Assert.assertEquals(currentValue, replicationStoreStats.getLastReplDownTime());

        sleep(50);
        replicationStoreStats.setMasterState(MASTER_STATE.REDIS_REPL_NONE);

        exptected = System.currentTimeMillis();
        Assert.assertTrue(replicationStoreStats.getLastReplDownTime() >= (exptected - 50)  && replicationStoreStats.getLastReplDownTime() <= (exptected + 50));
    }


}
