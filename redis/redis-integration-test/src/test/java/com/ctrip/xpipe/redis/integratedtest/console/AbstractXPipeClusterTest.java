package com.ctrip.xpipe.redis.integratedtest.console;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.AbstractConsoleDbTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareResponse;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DoRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DoResponse;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthChecker;

import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.integratedtest.console.app.ConsoleApp;
import com.ctrip.xpipe.redis.integratedtest.console.app.MetaserverApp;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisKillCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.ServerStartCmd;
import com.ctrip.xpipe.redis.keeper.KeeperContainerApplication;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.FileUtils;
import org.junit.Assert;
import org.springframework.web.client.RestOperations;

import java.io.File;
import java.util.*;
import java.util.stream.IntStream;

import static com.ctrip.xpipe.foundation.DefaultFoundationService.DATA_CENTER_KEY;
import static com.ctrip.xpipe.redis.checker.config.CheckerConfig.KEY_CHECKER_META_REFRESH_INTERVAL;
import static com.ctrip.xpipe.redis.checker.config.CheckerConfig.KEY_SENTINEL_CHECK_INTERVAL;
import static com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig.KEY_METASERVERS;
import static com.ctrip.xpipe.redis.core.config.AbstractCoreConfig.KEY_ZK_ADDRESS;
import static com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig.KEY_REPLICATION_STORE_COMMANDFILE_SIZE;
import static com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig.KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB;
import static com.ctrip.xpipe.redis.keeper.config.DefaultKeeperContainerConfig.REPLICATION_STORE_DIR;
import static com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig.KEY_CONSOLE_ADDRESS;

/**
 * @author lishanglin
 * date 2021/2/3
 */
public abstract class AbstractXPipeClusterTest extends AbstractConsoleDbTest {

    protected RestOperations restTemplate;

    private List<ForkProcessCmd> subProcessCmds;

    private List<Integer> redisPorts;

    @Override
    public void before() {
        restTemplate = RestTemplateFactory.createRestTemplate();
        subProcessCmds = new ArrayList<>();
        redisPorts = new ArrayList<>();
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

        redisPorts.add(port);
        subProcessCmds.add(redis);
        return redis;
    }

    protected RedisStartCmd startSentinel(int port) {
        RedisStartCmd redis = new RedisStartCmd(port, true, executors);
        redis.execute(executors).addListener(redisFuture -> {
            if (redisFuture.isSuccess()) {
                logger.info("[startSentinel] sentinel{} end {}", port, redisFuture.get());
            } else {
                logger.info("[startSentinel] sentinel{} fail", port, redisFuture.cause());
            }
        });

        redisPorts.add(port);
        subProcessCmds.add(redis);
        return redis;
    }

    protected ServerStartCmd startConsole(int port, String idc, String zk, List<String> localDcConsoles,
                                          Map<String, String> crossDcConsoles, Map<String, String> metaservers) {
        return startConsole(port, idc, zk, localDcConsoles, crossDcConsoles, metaservers, Collections.emptyMap());
    }

    protected ServerStartCmd startConsole(int port, String idc, String zk, List<String> localDcConsoles,
                                          Map<String, String> crossDcConsoles, Map<String, String> metaservers,
                                          Map<String, String> extras) {
        ServerStartCmd consoleServer = new ServerStartCmd(idc + port, ConsoleApp.class.getName(), new HashMap<String, String>() {{
            put(HealthChecker.ENABLED, "true");
            put("server.port", String.valueOf(port));
            put("cat.client.enabled", "false");
            put("spring.profiles.active", AbstractProfile.PROFILE_NAME_PRODUCTION);
            put(DATA_CENTER_KEY, idc);
            put(KEY_ZK_ADDRESS, zk);
            put(KEY_METASERVERS, JsonCodec.INSTANCE.encode(metaservers));
            put("console.domains", JsonCodec.INSTANCE.encode(crossDcConsoles));
            put("console.all.addresses", String.join(",", localDcConsoles));
            put(KEY_CHECKER_META_REFRESH_INTERVAL, "2000");
            put(KEY_SENTINEL_CHECK_INTERVAL, "15000");
            putAll(extras);
        }}, executors);
        consoleServer.execute(executors).addListener(consoleFuture -> {
            if (consoleFuture.isSuccess()) {
                logger.info("[startConsoleJQ] console {}-{} end {}", idc, port, consoleFuture.get());
            } else {
                logger.info("[startConsoleJQ] console {}-{} fail", idc, port, consoleFuture.cause());
            }

        });

        subProcessCmds.add(consoleServer);
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

        subProcessCmds.add(metaserver);
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

        subProcessCmds.add(keepercontainer);
        return keepercontainer;
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

    /* --------- wait condition --------- */

    protected void waitForServerAck(String healthUrl, Class<?> respType, int waitTimeMilli) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try {
                Object resp = restTemplate.getForObject(healthUrl, respType);
                logger.info("[waitForServerAck] resp for {}, {}", healthUrl, resp);
                return true;
            } catch (Throwable th) {
                logger.info("[waitForServerAck] {} fail", healthUrl, th);
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

    protected void waitForRedisRole(String host, int port, Server.SERVER_ROLE expected, int waitTimeMilli) throws Exception {
        waitConditionUntilTimeOut(() -> {
            try {
                Role current = new RoleCommand(host, port, scheduled).execute().get();
                logger.info("[waitForRedisRole][{}][{}] current role {}", host, port, current);
                return current.getServerRole().equals(expected);
            } catch (Throwable th) {
                logger.info("[waitForRedisRole][{}][{}] role cmd fail", host, port, th);
                return false;
            }
        }, waitTimeMilli, 2000);
    }

    /* --------- cleanup --------- */

    protected boolean stopServer(ForkProcessCmd server) {
        if (subProcessCmds.remove(server)) {
            server.killProcess();
            return true;
        }

        return false;
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
        // TODO: override conf to be clean in sub-class
        IntStream.of(6379, 7379, 5000, 5001, 5002, 17170, 17171, 17172).forEach(port -> {
            File conf = new File(userDir + "/src/test/tmp/redis" + port + ".conf");
            try {
                if (conf.exists()) conf.delete();
                else logger.info("[cleanupConf][{}] no conf {}", port, conf.getAbsolutePath());
            } catch (Throwable th) {
                logger.info("[cleanupConf][{}] delete conf fail", port, th);
            }
        });
    }

    protected void cleanupAllSubProcesses() {
        for (ForkProcessCmd subProcess: subProcessCmds) {
            try {
                logger.info("[cleanupSubProcesses][{}]", subProcess);
                subProcess.killProcess();
            } catch (Throwable th) {
                logger.info("[cleanupSubProcesses][{}] kill thread fail", subProcess, th);
            }
        }
    }

    protected void checkAllProcessAlive() {
        for (ForkProcessCmd subProcess: subProcessCmds) {
            logger.info("[checkAllProcessAlive][{}]", subProcess);
            if (!subProcess.isProcessAlive()) {
                throw new IllegalStateException("sub process " + subProcess + " is down");
            }
        }
    }

    protected void killAllRedisServers() {
        redisPorts.forEach(port -> {
            try {
                new RedisKillCmd(port, executors).execute().get();
            } catch (Throwable th) {
                logger.info("[killRedisServers][{}] fail", port, th);
            }
        });
    }

}
