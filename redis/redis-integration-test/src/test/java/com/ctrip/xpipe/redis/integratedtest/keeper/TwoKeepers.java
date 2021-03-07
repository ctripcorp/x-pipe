package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryPsync;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedMultiDc;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Slight
 *
 *         Feb 21, 2021 5:45 PM
 */
public class TwoKeepers extends AbstractKeeperIntegratedMultiDc {

    @Before
    @Override
    public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{

        prepareCaseIfExist();

        for(DcMeta dcMeta : getDcMetas()){
            startZkServer(dcMeta.getZkServer());
        }

        setKeeperActive();
        startKeepers();

        sleep(3000);
    }

    private void startRedisMaster() throws IOException {
        for(DcMeta dcMeta : getDcMetas()){
            for(RedisMeta redisMeta : getDcRedises(dcMeta.getId(), getClusterId(), getShardId())){
                if (redisMeta.isMaster()) {
                    startRedis(redisMeta);
                }
            }
        }
    }

    private boolean psync(KeeperMeta downstreamKeeper) throws InterruptedException, ExecutionException {
        Psync psync = new InMemoryPsync(downstreamKeeper.getIp(), downstreamKeeper.getPort(), "?", -1, scheduled);
        try {
            return psync.execute().sync().isSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    public void loopReplicating() throws Exception {

        RedisMeta redis = getRedisMaster();
        KeeperMeta upstreamKeeper = getKeeperActive("jq");
        KeeperMeta downstreamKeeper = getKeeperActive("oy");

        setKeeperState(upstreamKeeper, KeeperState.ACTIVE, redis.getIp(), redis.getPort());
        setKeeperState(downstreamKeeper, KeeperState.ACTIVE, upstreamKeeper.getIp(), upstreamKeeper.getPort());

        while (!psync(downstreamKeeper)) {
            sleep(100);
        }
    }

    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1 << 30,
                1<<10,
                1 << 30,
                300000);
    }

}
