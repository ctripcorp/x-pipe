package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 * Mar 17, 2021
 */
public class DefaultMasterStatsTest extends AbstractTest {

    @Test
    public void test() {

        DefaultMasterStats masterStats = new DefaultMasterStats();
        masterStats.increaseDefaultReplicationInputBytes(100);
        Assert.assertEquals(0, masterStats.getCommandsLength());

        masterStats.setMasterState(MASTER_STATE.REDIS_REPL_HANDSHAKE);
        masterStats.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        masterStats.increaseDefaultReplicationInputBytes(100);
        sleep(10);
        Assert.assertTrue(masterStats.getCommandBPMilli() > 5 && masterStats.getCommandBPMilli() < 15);

        masterStats.setMasterState(MASTER_STATE.REDIS_REPL_NONE);
        Assert.assertTrue(masterStats.getCommandBPMilli() > 5 && masterStats.getCommandBPMilli() < 15);


    }
}
