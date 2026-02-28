package com.ctrip.xpipe.redis.integratedtest.applier;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.integratedtest.keeper.AbstractKeeperIntegrated;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.applier.ApplierConfig;
import com.ctrip.xpipe.redis.keeper.applier.DefaultApplierServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.meta.server.job.KeeperStateChangeJob;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;

/**
 * @author TB
 * @date 2026/2/26 15:52
 */
public class AbstractApplierIntegratedSingleDc extends AbstractKeeperIntegrated {
    protected String dc = "jq";
    private LeaderElectorManager leaderElectorManager;
    protected FakeXsyncServer server;
    private RedisOpParserManager redisOpParserManager;

    private RedisOpParser parser;
    protected DefaultApplierServer applier;
    @Before
    public void beforeAbstractApplierIntegratedSingleDc() throws Exception{
        initParser();

        startZkServer(getDcMeta(dc).getZkServer());

        setFistKeeperActive();

        initResource();

        startRedises();
        flushRedises();

        int keeperPort = startKeeper();

        startApplier();

        server = startFakeXsyncServer(randomPort(), null);

        makeKeeperRight("127.0.0.1",server.getPort());

        ApplierConfig config = new ApplierConfig();
        config.setDropAllowKeys(-1);
        config.setDropAllowRation(-1);
        config.setUseXsync(true);
        applier.setStateActive(new DefaultEndPoint("127.0.0.1", keeperPort), new GtidSet("a1:1-10:15-20,b1:1-8"), config);

        sleep(3000);//wait for structure to build
    }

    private void initParser(){
        redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        parser = new GeneralRedisOpParser(redisOpParserManager);
    }

    private void initResource() throws Exception {

        DcMeta dcMeta = getDcMeta(dc);
        leaderElectorManager = createLeaderElectorManager(dcMeta);
    }

    private void setFistKeeperActive() {
        getDcKeepers(dc, getClusterId(), getShardId()).get(0).setActive(true);
    }

    protected void startRedises() throws IOException {
        for(RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())){
            startRedis(redisMeta);
        }
    }

    protected int startKeeper() throws Exception{
        KeeperMeta keeperMeta = getDcKeepers(dc, getClusterId(), getShardId()).get(0);
        RedisKeeperServer redisKeeperServer = startKeeper(keeperMeta, leaderElectorManager);
        return redisKeeperServer.getListeningPort();
    }

    protected void startApplier() throws Exception{
        ApplierMeta applierMeta = getDcAppliers(dc, getClusterId(), getShardId()).get(0);

        applier = new DefaultApplierServer(
                "ApplierTest",
                ClusterId.from(1L), ShardId.from(1L),
                applierMeta, leaderElectorManager, parser, new TestKeeperConfig(),1,1,
                50000l, 167772160l, 10l, 10000l, null,2);
        applier.initialize();
        applier.start();
    }

    protected void makeKeeperRight(String ip,int port) throws Exception {

        List<KeeperMeta> keepers = getDcKeepers(dc, getClusterId(), getShardId());

        KeeperStateChangeJob job = new KeeperStateChangeJob(keepers, new Pair<String, Integer>(ip, port), null, getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
        job.execute().sync();
    }

    @Override
    protected List<RedisMeta> getRedisSlaves() {
        return List.of();
    }

    protected String getXpipeMetaConfigFile() {
        return "integrated-keeper-applier-test.xml";
    }

    private void flushRedises(){
        for(RedisMeta redisMeta : getDcRedises(dc, getClusterId(), getShardId())){
            Jedis jedis = new Jedis(redisMeta.getIp(),redisMeta.getPort());
            jedis.flushAll();
        }
    }

}
