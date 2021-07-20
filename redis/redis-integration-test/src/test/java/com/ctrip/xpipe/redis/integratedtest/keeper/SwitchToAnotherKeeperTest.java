package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.Redis;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Slight
 * <p>
 * Jul 08, 2021 11:45 PM
 */
public class SwitchToAnotherKeeperTest extends AbstractKeeperIntegratedMultiDc {


    @Before
    @Override
    public void beforeAbstractKeeperIntegratedSingleDc() throws Exception {

        for (DcMeta dcMeta : getDcMetas()) {
            startZkServer(dcMeta.getZkServer());
        }

        setKeeperActive();
        startKeepers();

        sleep(3000);
    }

    @Override
    protected void endPrepareRedisConfig(RedisMeta redisMeta, StringBuilder sb) {
        //DO-NOTHING
    }

    @Test
    public void errorSwitchMaster() throws Exception {

        RedisMeta master = getRedisMaster();
        RedisMeta slave = getRedisSlaves("jq").get(2);

        startRedis(master);
        startRedis(slave, master);

        KeeperMeta upstreamKeeper = getKeeperActive("jq");

        setKeeperState(upstreamKeeper, KeeperState.ACTIVE, master.getIp(), master.getPort());
        setKeeperState(upstreamKeeper, KeeperState.ACTIVE, slave.getIp(), slave.getPort());
        System.out.println("finished");
    }
}
