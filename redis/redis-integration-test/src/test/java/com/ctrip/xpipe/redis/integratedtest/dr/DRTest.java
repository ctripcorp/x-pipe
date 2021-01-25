package com.ctrip.xpipe.redis.integratedtest.dr;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.AbstractConsoleH2DbTest;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareResponse;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DoRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DoResponse;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.integratedtest.dr.app.ConsoleApp;
import com.ctrip.xpipe.redis.integratedtest.dr.app.MetaserverApp;
import com.ctrip.xpipe.redis.integratedtest.dr.cmd.RedisKillCmd;
import com.ctrip.xpipe.redis.integratedtest.dr.cmd.RedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.dr.cmd.ServerStartCmd;
import com.ctrip.xpipe.redis.keeper.KeeperContainerApplication;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import static com.ctrip.xpipe.foundation.DefaultFoundationService.DATA_CENTER_KEY;
import static com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig.KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK;
import static com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig.KEY_METASERVERS;
import static com.ctrip.xpipe.redis.core.config.AbstractCoreConfig.KEY_ZK_ADDRESS;
import static com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig.KEY_REPLICATION_STORE_COMMANDFILE_SIZE;
import static com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB;
import static com.ctrip.xpipe.redis.keeper.config.DefaultKeeperContainerConfig.REPLICATION_STORE_DIR;
import static com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig.KEY_CONSOLE_ADDRESS;

/**
 * @author lishanglin
 * date 2021/1/21
 */
public class DRTest extends AbstractConsoleH2DbTest {

    private RestOperations restTemplate;

    private Map<String, DcInfo> dcInfos;

    private Map<String, String> consoles;

    private Map<String, String> metaservers;

    private List<ForkProcessCmd> subProcessCmds;

    private String zkJQ;

    private String zkOY;

    @Override
    public void before() {
        restTemplate = RestTemplateFactory.createRestTemplate();

        subProcessCmds = new ArrayList<>();

        dcInfos = new HashMap<>();
        dcInfos.put("jq", new DcInfo("http://127.0.0.1:" + IdcUtil.JQ_METASERVER_PORT));
        dcInfos.put("oy", new DcInfo("http://127.0.0.1:" + IdcUtil.OY_METASERVER_PORT));

        consoles = new HashMap<>();
        consoles.put("jq", "http://127.0.0.1:8080");
        consoles.put("oy", "http://127.0.0.1:8081");

        metaservers = new HashMap<>();
        metaservers.put("jq", dcInfos.get("jq").getMetaServerAddress());
        metaservers.put("oy", dcInfos.get("oy").getMetaServerAddress());

        zkJQ = "127.0.0.1:" + IdcUtil.JQ_ZK_PORT;
        zkOY = "127.0.0.1:" + IdcUtil.OY_ZK_PORT;
    }

    @After
    public void afterDRTest() throws IOException {
        cleanupSubProcesses();
        killRedisServers();
        cleanupConf();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-dr.sql");
    }

    @Test
    public void startSimpleXPipeDR() throws Exception {
        startZk(IdcUtil.JQ_ZK_PORT);
        startZk(IdcUtil.OY_ZK_PORT);

        startH2Server();
        setUpTestDataSource(); // init data in h2

        subProcessCmds.add(startRedis(6379));
        subProcessCmds.add(startRedis(7379));
        subProcessCmds.add(startConsole(8080, "jq", zkJQ, consoles, metaservers));
        subProcessCmds.add(startConsole(8081, "oy", zkOY, consoles, metaservers));

        String userDir = System.getProperty("user.dir");
        subProcessCmds.add(startKeepercontainer("jq", zkJQ, 7080, userDir + "/src/test/tmp/keepercontainer7080"));
        subProcessCmds.add(startKeepercontainer("jq", zkJQ, 7081, userDir + "/src/test/tmp/keepercontainer7081"));
        subProcessCmds.add(startKeepercontainer("oy", zkOY, 7180, userDir + "/src/test/tmp/keepercontainer7180"));
        subProcessCmds.add(startKeepercontainer("oy", zkOY, 7181, userDir + "/src/test/tmp/keepercontainer7181"));

        // wait for console init
        waitForServerInit("http://localhost:8080/api/dc/jq", DcMeta.class, 120000);
        waitForServerInit("http://localhost:8081/api/dc/oy", DcMeta.class, 60000);

        checkAllProcessAlive();

        subProcessCmds.add(startMetaServer("jq", "http://127.0.0.1:8080", zkJQ, IdcUtil.JQ_METASERVER_PORT, dcInfos));
        subProcessCmds.add(startMetaServer("oy", "http://127.0.0.1:8081", zkOY, IdcUtil.OY_METASERVER_PORT, dcInfos));

        // repl online
        waitForServerRespAsExpected("http://localhost:8080/api/health/127.0.0.1/7379", String.class, "\"HEALTHY\"", 120000);

        checkAllProcessAlive();

        // check migration system
        waitForServerRespAsExpected("http://localhost:8080/api/migration/migration/system/health/status", RetMessage.class, RetMessage.createSuccessMessage(), 60000);

        // do migration
        tryMigration("http://localhost:8080", "cluster-dr", "jq", "oy");

        // check result
        waitForServerRespAsExpected("http://localhost:8081/api/health/127.0.0.1/6379", String.class, "\"HEALTHY\"", 30000);
    }

    protected RedisStartCmd startRedis(int port) {
        RedisStartCmd redis = new RedisStartCmd(port, executors);
        redis.execute(executors).addListener(redisFuture -> {
            if (redisFuture.isSuccess()) {
                logger.info("[startRedis] redis{} end {}", port, redisFuture.get());
            } else {
                logger.info("[startRedis] redis{} fail", port, redisFuture.cause());
            }
        });

        return redis;
    }

    protected ServerStartCmd startConsole(int port, String idc, String zk, Map<String, String> consoles, Map<String, String> metaservers) {
        ServerStartCmd consoleServer = new ServerStartCmd(idc + port, ConsoleApp.class.getName(), new HashMap<String, String>() {{
            put(HealthChecker.ENABLED, "true");
            put("server.port", String.valueOf(port));
            put("cat.client.enabled", "false");
            put("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
            put(DATA_CENTER_KEY, idc);
            put(KEY_ZK_ADDRESS, zk);
            put(KEY_CLUSTER_SHARD_FOR_MIGRATE_SYS_CHECK, "cluster-dr,cluster-dr-shard1");
            put(KEY_METASERVERS, JsonCodec.INSTANCE.encode(metaservers));
            put("console.domains", JsonCodec.INSTANCE.encode(consoles));
        }}, executors);
        consoleServer.execute(executors).addListener(consoleFuture -> {
            if (consoleFuture.isSuccess()) {
                logger.info("[startConsoleJQ] console {}-{} end {}", idc, port, consoleFuture.get());
            } else {
                logger.info("[startConsoleJQ] console {}-{} fail", idc, port, consoleFuture.cause());
            }

        });

        return consoleServer;
    }

    protected ServerStartCmd startMetaServer(String idc, String console, String zk, int port, Map<String, DcInfo> dcInfos) {
        ServerStartCmd metaserver = new ServerStartCmd(idc + port, MetaserverApp.class.getName(), new HashMap<String, String>() {{
            put("server.port", String.valueOf(port));
            put("cat.client.enabled", "false");
            put("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
            put(DATA_CENTER_KEY, idc);
            put(KEY_CONSOLE_ADDRESS, console);
            put(KEY_ZK_ADDRESS, zk);
            put(DefaultMetaServerConfig.KEY_DC_INFOS, JsonCodec.INSTANCE.encode(dcInfos));
        }}, executors);
        metaserver.execute(executors).addListener(metaserverFuture -> {
            if (metaserverFuture.isSuccess()) {
                logger.info("[startMetaServer] metaserver {}-{} end {}", idc, port, metaserverFuture.get());
            } else {
                logger.info("[startMetaServer] metaserver {}-{} fail", idc, port, metaserverFuture.cause());
            }
        });

        return metaserver;
    }

    protected ServerStartCmd startKeepercontainer(String idc, String zk, int port, String storeDir) {
        ServerStartCmd keepercontainer = new ServerStartCmd(idc + port, KeeperContainerApplication.class.getName(), new HashMap<String, String>() {{
            put("server.port", String.valueOf(port));
            put("cat.client.enabled", "false");
            put("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
            put(DATA_CENTER_KEY, idc);
            put(KEY_ZK_ADDRESS, zk);
            put(REPLICATION_STORE_DIR, storeDir);
            put(KEY_REPLICATION_STORE_COMMANDFILE_SIZE, "104857600");
            put(KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB, "524288000");
        }}, executors);
        keepercontainer.execute(executors).addListener(keepercontainerFuture -> {
            if (keepercontainerFuture.isSuccess()) {
                logger.info("[startKeepercontainer] metaserver {}-{} end {}", idc, port, keepercontainerFuture.get());
            } else {
                logger.info("[startKeepercontainer] metaserver {}-{} fail", idc, port, keepercontainerFuture.cause());
            }
        });

        return keepercontainer;
    }

    protected void waitForServerInit(String healthUrl, Class<?> respType, int waitTimeMilli) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try {
                Object resp = restTemplate.getForObject(healthUrl, respType);
                logger.info("[waitForServerInit] resp for {}, {}", healthUrl, resp);
                return true;
            } catch (Throwable th) {
                return false;
            }
        }, waitTimeMilli, 2000);
    }

    protected void waitForServerRespAsExpected(String healthUrl, Class<?> respType, Object expected, int waitTimeMilli) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try {
                Object resp = restTemplate.getForObject(healthUrl, respType);
                logger.info("[waitForServerRespAsExpected] resp for {}, {}", healthUrl, resp);
                return expected.equals(resp);
            } catch (Throwable th) {
                return false;
            }
        }, waitTimeMilli, 2000);
    }

    protected void tryMigration(String console, String cluster, String src, String dest) {
        CheckPrepareRequest prepareRequest = new CheckPrepareRequest();
        prepareRequest.setClusters(Collections.singletonList(cluster));
        prepareRequest.setFromIdc(src);
        prepareRequest.setToIdc(dest);
        prepareRequest.setIsForce(false);

        CheckPrepareResponse prepareResp = restTemplate.postForObject(console + "/api/migration/checkandprepare", prepareRequest, CheckPrepareResponse.class);
        if (!prepareResp.getResults().get(0).isSuccess()) Assert.fail("migration prepare fail " + prepareResp.getResults().get(0).getFailReason());

        DoRequest doRequest = new DoRequest();
        doRequest.setTicketId(prepareResp.getTicketId());
        DoResponse doResp = restTemplate.postForObject(console + "/api/migration/domigration", doRequest, DoResponse.class);

        if (!doResp.isSuccess()) Assert.fail("do migration fail " + doResp.getMsg());
    }

    protected void cleanupKeeper() {
        String userDir = System.getProperty("user.dir");
        IntStream.of(7080, 7081, 7082, 7180, 7181, 7182).forEach(port -> {
            File dir = new File(userDir + "/src/test/tmp/keepercontainer" + port);
            if (dir.exists()) {
                try {
                    FileUtils.recursiveDelete(dir);
                } catch (Throwable th) {
                    logger.info("[cleanupKeeper][{}] delete dir fail", port, th);
                }
            } else {
                logger.info("[cleanupKeeper][{}] no rsd dir {}", port, dir.getAbsolutePath());
            }
        });
    }

    protected void cleanupRDB() {
        String userDir = System.getProperty("user.dir");
        IntStream.of(6379, 7379).forEach(port -> {
            File rdb = new File(userDir + "/src/test/tmp/dump" + port + ".rdb");
            try {
                if (rdb.exists()) rdb.delete();
                else logger.info("[cleanupRDB][{}] no rdb {}", port, rdb.getAbsolutePath());
            } catch (Throwable th) {
                logger.info("[cleanupRDB][{}] delete rdb fail", port, th);
            }
        });
    }

    protected void cleanupConf() {
        String userDir = System.getProperty("user.dir");
        IntStream.of(6379, 7379).forEach(port -> {
            File conf = new File(userDir + "/src/test/tmp/redis" + port + ".conf");
            try {
                if (conf.exists()) conf.delete();
                else logger.info("[cleanupConf][{}] no conf {}", port, conf.getAbsolutePath());
            } catch (Throwable th) {
                logger.info("[cleanupConf][{}] delete conf fail", port, th);
            }
        });
    }

    protected void cleanupSubProcesses() {
        for (ForkProcessCmd subProcess: subProcessCmds) {
            try {
                logger.info("[cleanupSubProcesses][{}]", subProcess);
                subProcess.killProcess();
            } catch (Throwable th) {
                logger.info("[cleanupSubProcesses][{}] kill thread fail", subProcess, th);
            }
        }
    }

    protected void killRedisServers() {
        IntStream.of(6379, 7379).forEach(port -> {
            try {
                new RedisKillCmd(port, executors).execute().get();
            } catch (Throwable th) {
                logger.info("[killRedisServers][{}] fail", port, th);
            }
        });
    }

    protected void checkAllProcessAlive() {
        for (ForkProcessCmd subProcess: subProcessCmds) {
            logger.info("[checkAllProcessAlive][{}]", subProcess);
            if (!subProcess.isProcessAlive()) {
                throw new IllegalStateException("sub process " + subProcess + " is down");
            }
        }
    }

}
