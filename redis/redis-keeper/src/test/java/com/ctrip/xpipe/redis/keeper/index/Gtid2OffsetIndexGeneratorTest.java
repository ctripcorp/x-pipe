package com.ctrip.xpipe.redis.keeper.index;

import com.ctrip.xpipe.api.lifecycle.ComponentRegistry;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.CreatedComponentRedistry;
import com.ctrip.xpipe.lifecycle.DefaultRegistry;
import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.XsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserFactory;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.DefaultRedisOpParserManager;
import com.ctrip.xpipe.redis.core.redis.operation.parser.GeneralRedisOpParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.DefaultRdbParser;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.AppTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.container.ComponentRegistryHolder;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActive;
import io.netty.buffer.ByteBuf;
import org.junit.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Slight
 * <p>
 * Nov 08, 2022 13:32
 */
public class Gtid2OffsetIndexGeneratorTest extends AbstractRedisKeeperContextTest implements XsyncObserver, RdbParseListener {

    @Test
    public void manual() throws Exception {
        RedisKeeperServer redisKeeperServer = createRedisKeeperServer(createKeeperMeta(6000, "0"));
        redisKeeperServer.initialize();
        redisKeeperServer.start();

        redisKeeperServer.getReplicationStore().getMetaStore().becomeActive();
        redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, new DefaultEndPoint("127.0.0.1", 6379)));
        redisKeeperServer.reconnectMaster();

        sleep(5000);

        RedisMeta redis = new RedisMeta();
        redis.setIp("127.0.0.1");
        redis.setPort(6379);

        redisKeeperServer.startIndexing();

        sendRandomMessage(redis, 30);

        waitForAnyKey();
    }

    protected RedisOpParser createRedisOpParser() {
        RedisOpParserManager redisOpParserManager = new DefaultRedisOpParserManager();
        RedisOpParserFactory.getInstance().registerParsers(redisOpParserManager);
        RedisOpParser parser = new GeneralRedisOpParser(redisOpParserManager);
        return parser;
    }

    RedisOpParser cmdParser = createRedisOpParser();

    RdbParser<?> rdbParser;

    @Test
    public void xsync() throws Exception {

        rdbParser = new DefaultRdbParser();
        rdbParser.registerListener(this);

        XpipeNettyClientKeyedObjectPool pool = new XpipeNettyClientKeyedObjectPool(8);
        pool.initialize();
        pool.start();

        SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(new DefaultEndPoint("127.0.0.1", 6000));
        DefaultXsync xsync = new DefaultXsync(objectPool, new GtidSet("e0cbd7e2858c5b1f6216f22d1d24eecaeb1d48ef:0"), null, scheduled);
        xsync.addXsyncObserver(this);
        xsync.execute(executors);

        waitForAnyKey();
    }

    private void setReplicationStoreDir() {
        System.setProperty("replication.store.dir", String.format("/opt/data/xpipe%s", System.getProperty("server.port")));
    }

    private void start() throws Exception {
        ConfigurableApplicationContext context =
                new SpringApplicationBuilder(AppTest.class).run();
        initComponentRegistry(context);
        waitForAnyKeyToExit();
    }

    private void initComponentRegistry(ConfigurableApplicationContext  context) throws Exception {
        ComponentRegistry registry = new DefaultRegistry(new CreatedComponentRedistry(),
                new SpringComponentRegistry(context));
        registry.initialize();
        registry.start();
        ComponentRegistryHolder.initializeRegistry(registry);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "keeper-test.xml";
    }

    @Override
    protected KeeperConfig getKeeperConfig() {
        KeeperConfig config = super.getKeeperConfig();
        if (config instanceof TestKeeperConfig) {
            TestKeeperConfig modified = (TestKeeperConfig)config;
            modified.setReplicationStoreCommandFileSize(1024 * 1024 * 128 /* 128 M */);
            modified.setCommandReaderFlyingThreshold(10);
            modified.setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(1024 * 1024 * 1024);
            //modified.setReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb(0);
        }
        return config;
    }

    @Override
    protected boolean deleteTestDirBeforeTest() {
        return false;
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet) {
        logger.info(rdbGtidSet.toString());
    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onRdbData(ByteBuf rdbData) {
        try {
            rdbParser.read(rdbData);
        } catch (Throwable t){
            logger.error("[onRdbData] unlikely - error", t);
        }
    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet) {

    }

    @Override
    public void onContinue(GtidSet gtidSetExcluded) {

    }

    @Override
    public void onCommand(Object[] rawCmdArgs) {
        try {
            onRedisOp(cmdParser.parse(rawCmdArgs));
        } catch (Throwable unlikely) {
            logger.error("[onCommand] unlikely - when doing partial sync]", unlikely);
        }
    }

    @Override
    public void onRedisOp(RedisOp redisOp) {
        logger.info("{}: {}", redisOp, redisOp.estimatedSize());
    }

    @Override
    public void onAux(String key, String value) {

    }

    @Override
    public void onFinish(RdbParser<?> parser) {

    }
}