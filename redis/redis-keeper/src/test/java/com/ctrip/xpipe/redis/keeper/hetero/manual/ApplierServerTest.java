package com.ctrip.xpipe.redis.keeper.hetero.manual;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.applier.DefaultApplierServer;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Slight
 * <p>
 * Dec 13, 2022 09:59
 */
public class ApplierServerTest extends AbstractRedisKeeperTest {


    RedisOpParser cmdParser = createRedisOpParser();


    protected RedisOpParser createRedisOpParser() {
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser parser = new GeneralRedisOpParser(redisOpParserManager);
        return parser;
    }

    @Test
    public void applier() throws Exception {
        LeaderElectorManager leaderElectorManager = mock(LeaderElectorManager.class);
        when(leaderElectorManager.createLeaderElector(any())).thenReturn(mock(LeaderElector.class));

        ApplierMeta meta = new ApplierMeta();
        meta.setActive(true);
        meta.setApplierContainerId(1L);
        meta.setId("local-test-applier");
        meta.setIp("127.0.0.1");
        meta.setPort(8000);
        DefaultApplierServer applier = new DefaultApplierServer("ApplierTest", ClusterId.from(1L), ShardId.from(1L), meta, leaderElectorManager, cmdParser, getKeeperConfig());
        applier.initialize();
        applier.start();

        //applier.setStateBackup();

        applier.setStateActive(new DefaultEndPoint("127.0.0.1", 6000), new GtidSet("29097dd95625bc57c42bb0d8c887ec7bc847c05a:0"));

        //applier.setStateBackup();

        //applier.setStateActive(new DefaultEndPoint("127.0.0.1", 6000), new GtidSet("29097dd95625bc57c42bb0d8c887ec7bc847c05a:0"));

        waitForAnyKey();
    }
}
