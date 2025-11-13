package com.ctrip.xpipe.redis.integratedtest.metaserver;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultSlaveOfCommand;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisKillCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.ServerStartCmd;
import com.ctrip.xpipe.redis.proxy.ProxyServer;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.zk.ZkTestServer;
import org.springframework.web.client.RestOperations;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class AbstractXpipeServerMultiDcTest extends AbstractXpipeServerIntegrated {
    protected Map<String, String> metaservers = new HashMap<>();
    @Override
    protected String getXpipeMetaConfigFile() {
        return "crdt-full-test.xml";
    }
    int getPort(String uri) {
        String[] parts = uri.split(":");
        if (parts.length != 2) {
            throw new IllegalStateException("zk address wrong:" + uri);
        }
        int zkPort = Integer.parseInt(parts[1]);
        return zkPort;
    }
    Map<String, String> zkUrls = new HashMap<>();
    //start zk
    public ZkTestServer startZk(ZkServerMeta zkServerMeta) {
        String[] addresses = zkServerMeta.getAddress().split("\\s*,\\s*");
        if (addresses.length != 1) {
            throw new IllegalStateException("zk server test should only be one there!" + zkServerMeta.getAddress());
        }
        int zkPort = getPort(addresses[0]);
        return startZk(zkPort);
    }


    RedisStartCmd startSentinel(String address) {
        int port = getPort(address);
        return startSentinel(port);
    }

    protected List<RedisStartCmd> startSentinels(SentinelMeta meta) {
        String address = meta.getAddress();
        String[] ips = address.split(",");
        List<RedisStartCmd> results = new LinkedList<>();
        for(int i = 0, len = ips.length; i < len; i++) {
            results.add(startSentinel(ips[i]));
        }
        return results;
    }

    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/xpipe-crdt.sql");
    }

    protected void startDb() throws Exception {
        setUpTestDataSource();
    }

    protected int getGid(String idcName) throws Exception {
        switch (idcName) {
            case "rb":
                return 1;
            case "oy":
                return 2;
            case "fq":
                return 3;
            case "fra":
                return 5;
            case "jq":
                return 1;
            default:
                throw new Exception(new XpipeRuntimeException(String.format(" %s gid -> int error", idcName)));
        }
    }

    RedisStartCmd startCrdtRedis(int gid, RedisMeta meta) {
        return startCrdtRedis(gid, meta.getPort());
    }

    protected void stopRedis(RedisMeta meta) {
        try {
            new RedisKillCmd(meta.getPort(), executors).execute().get();
        } catch (Throwable th) {
            logger.info("[killRedisServers][{}] fail", meta.getPort(), th);
        }
    }
    List<ProxyServer> proxys = new LinkedList<>();
    protected void killAllProxyServers() throws Exception {
        for(ProxyServer proxy: proxys) {
            proxy.stop();
        }
        proxys = new LinkedList<>();
    }

    void startProxys() throws Exception {
        proxys.add(startProxyServer( 11080, 11443));
        proxys.add(startProxyServer( 11081, 11444));
    }

    void waitConsole(String url, String idc, int wait_time) throws Exception {
        waitForServerAck(String.format("http://%s/api/dc/%s", url, idc), DcMeta.class, wait_time);
    }

    Endpoint parseEndpoint(String uri) {
        String[] parts = uri.split(":");
        if (parts.length != 2) {
            throw new IllegalStateException("zk address wrong:" + uri);
        }
        int point = Integer.parseInt(parts[1]);
        return new DefaultEndPoint(parts[0], point);
    }

    Map<String, HealthServer> checks = new HashMap<>();
    class HealthServer {
        String url;
        ServerStartCmd server;
        HealthServer(String url, ServerStartCmd server) {
            this.url = url;
            this.server = server;
        }
        protected RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplateWithRetry(3, 100);
        boolean isClosed() {
            try {
                restTemplate.getForObject(this.url, String.class);
                return false;
            } catch (Exception e) {
                return true;
            }
        }
    }

    public void closeCheck(String idc) throws TimeoutException {
        HealthServer server = checks.get(idc);
        server.server.killProcess();
        waitConditionUntilTimeOut(server::isClosed, 100000, 1000);
    }

    public static class ConsoleInfo {
        ConsoleServerModeCondition.SERVER_MODE mode;
        int console_port;
        int checker_port;
        boolean enable = true;

        public ConsoleInfo(ConsoleServerModeCondition.SERVER_MODE mode) {
            this.mode = mode;
        }

        public ConsoleInfo setChecker_port(int checker_port) {
            this.checker_port = checker_port;
            return this;
        }

        public ConsoleInfo setConsole_port(int console_port) {
            this.console_port = console_port;
            return this;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        public boolean getEnable() {
            return enable;
        }

    }


    Map<String, String> consoles = new HashMap<>();
    public void startCRDTAllServer(Map<String, ConsoleInfo> consoleInfos) throws Exception {
        startDb();

        startProxys();

        Map<String, DcInfo> dcinfos = new HashMap<>();

        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();

        for(DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            String idc =  dcMeta.getId();
            //start zk
            ZkServerMeta zkmeta = dcMeta.getZkServer();
            startZk(zkmeta);
            zkUrls.put(idc, zkmeta.getAddress());
            //start sentinel
            Map<Long, SentinelMeta> sentinel_metas = dcMeta.getSentinels();
            sentinel_metas.entrySet().stream().forEach(sentinel_meta -> {
                startSentinels(sentinel_meta.getValue());
            });

            //start cluster redis
            Map<String, ClusterMeta> clusters = dcMeta.getClusters();
            int gid = getGid(idc);
            clusters.entrySet().stream().forEach(entry -> {
                String cluster_name = entry.getKey();
                ClusterMeta cluster_meta = entry.getValue();
                cluster_meta.getShards().entrySet().stream().forEach(shard_entry -> {
                    String shard = shard_entry.getKey();
                    ShardMeta shard_meta = shard_entry.getValue();
                    shard_meta.getRedises().stream().forEach(redis_meta-> {
                        stopRedis(redis_meta);
                        startCrdtRedis(gid, redis_meta);
                        if(!redis_meta.isMaster()) {
                            try {
                                Endpoint master_point= parseEndpoint(redis_meta.getMaster());
                                Command<String> command = new DefaultSlaveOfCommand(
                                        pool.getKeyPool(new DefaultEndPoint(redis_meta.getIp(), redis_meta.getPort())),
                                        master_point.getHost(), master_point.getPort(),
                                        scheduled);
                                Thread.sleep(2000);
                                command.execute().get();

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            } catch (ExecutionException e) {
                                e.printStackTrace();
                            }


                        }

                    });
                });
            });

            List<MetaServerMeta> metaservermetas  =  dcMeta.getMetaServers();
            String url = String.join(",", metaservermetas.stream().map(metaservermeta-> {
                return String.format("http://%s:%d", metaservermeta.getIp(), metaservermeta.getPort());
            }).collect(Collectors.toList()));
            metaservers.put(idc, url);
            DcInfo dcInfo = new DcInfo(url);
            dcinfos.put(idc, dcInfo);

        }

        consoleInfos.entrySet().stream().forEach(info -> {
            ConsoleInfo i = info.getValue();
            String console_url = String.format("http://127.0.0.1:" + i.console_port);
            consoles.put(info.getKey(), console_url);
        });

        //console info



//        Map<String, String> extraOptions = new HashMap<>();
//        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
//        extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
        for(DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            String idc =  dcMeta.getId();
            ConsoleInfo info = consoleInfos.get(idc);
            Map<String, String> extraOptions = new HashMap<>();
            extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
            if(info != null && info.enable) {
                switch (info.mode) {
                    case CONSOLE:
                        logger.info("================= start console server ==================");
                        // extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
                        startConsole(info.console_port, idc, dcMeta.getZkServer().getAddress(), Collections.singletonList(consoles.get(idc)), consoles, metaservers, extraOptions);
                        logger.info("================= start checker server ==================");
                        checks.put(idc, new HealthServer("http://127.0.0.1:"+ info.checker_port + "/health" , startChecker(info.checker_port, idc, dcMeta.getZkServer().getAddress(), Collections.singletonList(String.format("127.0.0.1:" + info.console_port)))));
                        break;
                    case CONSOLE_CHECKER:
                        logger.info("================= start console + checker server ==================");
                        // extraOptions.put(KEY_SERVER_MODE, CONSOLE_CHECKER.name());
                        ServerStartCmd console_checker = startConsole(info.console_port, idc, dcMeta.getZkServer().getAddress(), Collections.singletonList(consoles.get(idc)), consoles, metaservers, extraOptions);
                        checks.put(idc, new HealthServer("http://127.0.0.1:" + info.console_port + "/health", console_checker));
                        break;
                    case CHECKER:
                        logger.info("================= start checker server ==================");
                        checks.put(idc, new HealthServer("http://127.0.0.1:"+ info.checker_port + "/health" , startChecker(info.checker_port, idc, dcMeta.getZkServer().getAddress(), Collections.singletonList(String.format("127.0.0.1:" + info.console_port)))));
                        break;

                }

            }

        }
        for(DcMeta dcMeta: getXpipeMeta().getDcs().values()) {
            String idc = dcMeta.getId();
            ConsoleInfo info = consoleInfos.get(idc);
            waitConsole("127.0.0.1:" + info.console_port, idc, 180 * 1000);
            dcMeta.getMetaServers().stream().forEach(meta -> {
                logger.info("================= start metaserver {} ==================", meta.getPort());
                startMetaServer(idc, String.format("http://127.0.0.1:%d" , info.console_port),  dcMeta.getZkServer().getAddress(),  meta.getPort(), dcinfos);
            });
        }

        for (String url: metaservers.values()) {
            waitForServerAck(url + "/health", Boolean.class, 180 * 1000);
        }
    }

    public void stopAllServer() throws Exception {
        killAllProxyServers();
        cleanupAllSubProcesses();
        killAllRedisServers();
        cleanupConf();
    }
}
