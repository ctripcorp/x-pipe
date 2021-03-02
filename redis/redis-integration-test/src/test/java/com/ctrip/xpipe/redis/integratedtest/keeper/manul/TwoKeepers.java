package com.ctrip.xpipe.redis.integratedtest.keeper.manul;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegratedMultiDc;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Slight
 *
 *         Feb 21, 2021 5:45 PM
 */
public class TwoKeepers extends AbstractKeeperIntegratedMultiDc {

    @Before
    @Override
    public void beforeAbstractKeeperIntegratedSingleDc() throws Exception{

        for(DcMeta dcMeta : getDcMetas()){
            startZkServer(dcMeta.getZkServer());
        }

        startRedisMaster();
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

    @Test
    public void corruptedRdb() throws Exception {
        RedisMeta redis = getRedisMaster();
        KeeperMeta upstreamKeeper = getKeeperActive("jq");

        sendRandomMessage(redis, 100000);

        setKeeperState(upstreamKeeper, KeeperState.ACTIVE, redis.getIp(), redis.getPort());

        //sleep might be too short for keeper to begin receiving rdb
        // or too long for keeper to leave a rdb corrupted.
        //
        //adjust the time manually
        sleep(1000);

        setKeeperState(upstreamKeeper, KeeperState.ACTIVE, "127.0.0.1", 10000/*nothing there*/);

        waitForAnyKeyToExit();
    }

    @Test
    public void loopReplicating() throws Exception {

        RedisMeta redis = getRedisMaster();
        KeeperMeta upstreamKeeper = getKeeperActive("jq");
        KeeperMeta downstreamKeeper = getKeeperActive("oy");

        sendRandomMessage(redis, 10000);

        setKeeperState(upstreamKeeper, KeeperState.ACTIVE, redis.getIp(), redis.getPort());

        sendRandomMessage(redis, 90000);

        setKeeperState(downstreamKeeper, KeeperState.ACTIVE, redis.getIp(), redis.getPort());

        //sleep might be too short for keeper to begin receiving rdb
        // or too long for keeper to leave a rdb corrupted.
        //
        //adjust the time manually
        sleep(1150);

        setKeeperState(downstreamKeeper, KeeperState.ACTIVE, "127.0.0.1", 10000/*nothing there*/);

        setKeeperState(downstreamKeeper, KeeperState.ACTIVE, upstreamKeeper.getIp(), upstreamKeeper.getPort());

        waitForAnyKeyToExit();
    }

    protected KeeperConfig getKeeperConfig() {
        return new TestKeeperConfig(1 << 30,
                1<<10,
                1 << 30,
                300000);
    }

}
