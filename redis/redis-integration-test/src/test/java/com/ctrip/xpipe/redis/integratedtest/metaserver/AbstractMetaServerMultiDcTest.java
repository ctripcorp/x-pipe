package com.ctrip.xpipe.redis.integratedtest.metaserver;


import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.foundation.IdcUtil;
import com.ctrip.xpipe.redis.core.meta.DcInfo;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultSlaveOfCommand;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisKillCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.RedisStartCmd;
import com.ctrip.xpipe.redis.integratedtest.console.cmd.ServerStartCmd;
import com.ctrip.xpipe.redis.meta.server.MetaServer;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.zk.ZkTestServer;
import org.apache.commons.collections.map.HashedMap;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.KEY_SERVER_MODE;
import static com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition.SERVER_MODE.CONSOLE;

public class AbstractMetaServerMultiDcTest extends AbstractMetaServerIntegrated {
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
    ZkTestServer startZk(ZkServerMeta zkServerMeta) {
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

    List<RedisStartCmd> startSentinels(SentinelMeta meta) {
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

    public void startDb() throws Exception {
        //start db
//        startH2Server();
        setUpTestDataSource(); // init data in h2

//        xml no proxy info

    }

    int getGid(String idcName) throws Exception {
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

    void stopRedis(RedisMeta meta) {
        try {
            new RedisKillCmd(meta.getPort(), executors).execute().get();
        } catch (Throwable th) {
            logger.info("[killRedisServers][{}] fail", meta.getPort(), th);
        }
    }

    void startProxys() throws Exception {
        //start proxy
        startProxyServer( 11080, 11443);
        startProxyServer( 11081, 11444);
    }

    public void waitConsole(String url, String idc, int wait_time) throws Exception {
        waitForServerAck(String.format("http://%s/api/dc/%s", url, idc), DcMeta.class, wait_time);
    }

    RedisMeta findMaster(List<RedisMeta> lists) {
        for(RedisMeta r : lists) {
            if(r.isMaster()) {
                return r;
            }
        }
        return null;
    }

    Endpoint parseEndpoint(String uri) {
        String[] parts = uri.split(":");
        if (parts.length != 2) {
            throw new IllegalStateException("zk address wrong:" + uri);
        }
        int point = Integer.parseInt(parts[1]);
        return new DefaultEndPoint(parts[0], point);
    }

    Map<String, ServerStartCmd> checks = new HashMap<>();
    void closeCheck(String idc) {
        ServerStartCmd cmd = checks.get(idc);
        cmd.killProcess();
    }

    void startCRDTAllServer() throws Exception {
        startDb();

        startProxys();

        Map<String, DcInfo> dcinfos = new HashMap<>();
        Map<String, String> consoles = new HashMap<>();
        XpipeNettyClientKeyedObjectPool pool = getXpipeNettyClientKeyedObjectPool();

        int console_port = 18080;
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

            String console_url = String.format("127.0.0.1:" + console_port);
            consoles.put(idc, String.format(console_url));
            console_port++;
        }

        //console info



        Map<String, String> extraOptions = new HashMap<>();
        extraOptions.put("console.cluster.types", "one_way,bi_direction,ONE_WAY,BI_DIRECTION");
        extraOptions.put(KEY_SERVER_MODE, CONSOLE.name());
        for(DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            String idc =  dcMeta.getId();
            String uri = consoles.get(idc);
            int cp = getPort(uri);
            startConsole(cp, idc, dcMeta.getZkServer().getAddress(), Collections.singletonList(consoles.get(idc)), consoles, metaservers, extraOptions);
        }
        int checker_port = 28080;
        for(DcMeta dcMeta: getXpipeMeta().getDcs().values()) {
            String idc = dcMeta.getId();
            waitConsole(consoles.get(idc), idc, 200000);
            dcMeta.getMetaServers().stream().forEach(meta -> {
                startMetaServer(idc, String.format("http://%s",consoles.get(idc) ),  dcMeta.getZkServer().getAddress(),  meta.getPort(), dcinfos);
            });
            checks.put(idc, startChecker(checker_port++, idc, dcMeta.getZkServer().getAddress(), Collections.singletonList(consoles.get(idc))));
        }

    }

    void stopAllServer() {
        cleanupAllSubProcesses();
        killAllRedisServers();
        cleanupConf();
    }
}
