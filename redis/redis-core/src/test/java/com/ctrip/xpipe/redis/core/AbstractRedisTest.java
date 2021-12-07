package com.ctrip.xpipe.redis.core;


import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.junit.Assert;
import org.junit.Before;
import org.xml.sax.SAXException;
import redis.clients.jedis.Jedis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *         <p>
 *         2016年3月28日 下午5:54:09
 */
public abstract class AbstractRedisTest extends AbstractTest {


    protected static final int runidLength = 40;

    private AtomicLong totalSendMessageCount = new AtomicLong();

    private XpipeMeta xpipeMeta;

    @Before
    public void beforeAbstractRedisTest() throws SAXException, IOException {
        xpipeMeta = loadXpipeMeta(getXpipeMetaConfigFile());
    }


    protected String getXpipeMetaConfigFile() {
        return null;
    }


    protected String readLine(InputStream ins) throws IOException {

        StringBuilder sb = new StringBuilder();
        int last = 0;
        while (true) {

            int data = ins.read();
            if (data == -1) {
                return null;
            }
            sb.append((char) data);
            if (data == '\n' && last == '\r') {
                break;
            }
            last = data;
        }

        return sb.toString();
    }


    protected Jedis createJedis(RedisMeta redisMeta) {

        String ip = StringUtil.isEmpty(redisMeta.getIp()) ? "localhost" : redisMeta.getIp();
        Jedis jedis = new Jedis(ip, redisMeta.getPort());
        logger.info("[createJedis]{}", redisMeta.desc());
        return jedis;
    }

    protected Jedis createJedis(String addr) {
        InetSocketAddress address = IpUtils.parseSingle(addr);
        return createJedis(address.getHostString(), address.getPort());
    }

    protected Jedis createJedis(String host, int port) {
        Jedis jedis = new Jedis(host, port);
        logger.info("[createJedis]{}", jedis);
        return jedis;
    }

    protected Jedis createJedis(InetSocketAddress address) {

        Jedis jedis = new Jedis(address.getHostString(), address.getPort());
        logger.info("[createJedis]{}", jedis);
        return jedis;
    }

    protected boolean checkVersion(InetSocketAddress redis, String version) {

        try (Jedis jedis = createJedis(redis)) {
            String server = jedis.info("server");
            for (String line : server.split("\r*\n+")) {
                String[] part = line.split("\\s*:\\s*");
                if (part.length == 2 && part[1].equals(version)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected String getXRedisVersion(RedisMeta redis) {

        try (Jedis jedis = createJedis(redis)) {

            String server = jedis.info("server");
            return new InfoResultExtractor(server).extract("xredis_version");
        }
    }

    protected String toRedisProtocalString(String str) {

        ByteBuf format = new CommandBulkStringParser(str).format();
        return ByteBufUtils.readToString(format);

    }

    protected void assertRedisEquals(RedisMeta redisMaster, List<RedisMeta> slaves) {

        Map<String, String> values = new HashMap<>();
        Jedis jedis = createJedis(redisMaster);
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            values.put(key, jedis.get(key));
        }

        for (RedisMeta redisSlave : slaves) {

            logger.info(remarkableMessage("[assertRedisEquals]redisSlave:" + redisSlave));

            Jedis slave = createJedis(redisSlave);
            Set<String> slaveKeys = slave.keys("*");

            Pair<Set<String>, Set<String>> diff = diff(values.keySet(), slaveKeys);
            boolean fail = false;

            if (diff.getKey().size() != 0) {
                logger.info("[lack keys]{}", diff.getKey());
                printKeysAround(diff.getKey(), slave);
                fail = true;
            }
            if (diff.getValue().size() != 0) {
                logger.info("[more keys]{}", diff.getKey());
                fail = true;
            }

            Assert.assertFalse(fail);

            Set<String> keysWrong = new HashSet<>();
            for (Entry<String, String> entry : values.entrySet()) {
                String realValue = slave.get(entry.getKey());
                if (!realValue.equals(entry.getValue())) {
                    keysWrong.add(entry.getKey());
                }
            }

            if (keysWrong.size() != 0) {
                logger.info("[keysWrong]{}", keysWrong);
            }
            Assert.assertEquals(0, keysWrong.size());
        }
    }

    private void printKeysAround(Set<String> keys, Jedis slave) {

        for (String key : keys) {
            try {
                Integer keyInt = Integer.parseInt(key);
                for (int i = keyInt - 1; i <= keyInt + 1; i++) {
                    logger.info("[printKeysAround]{}, {}, {}", slave, i, slave.get(String.valueOf(i)));
                }
            } catch (Throwable th) {
                logger.error("[printKeysAround]" + key + "," + slave, th);
            }
        }
    }


    private Pair<Set<String>, Set<String>> diff(Set<String> keySet1, Set<String> keySet2) {

        Set<String> keySet1Copy = new HashSet<>(keySet1);
        Set<String> keySet2Copy = new HashSet<>(keySet2);

        keySet1Copy.removeAll(keySet2);
        keySet2Copy.removeAll(keySet1);

        return new Pair<>(keySet1Copy, keySet2Copy);
    }


    protected void sendRandomMessage(RedisMeta redisMeta, int count) {

        sendRandomMessage(redisMeta, count, 1024);
    }

    protected void sendRandomMessage(RedisMeta redisMeta, int count, int messageLength) {

        Jedis jedis = createJedis(redisMeta);
        logger.info("[sendRandomMessage][begin]{}", redisMeta.desc());
        for (int i = 0; i < count; i++) {

            long currentIndex = totalSendMessageCount.incrementAndGet();
            jedis.set(String.valueOf(currentIndex), randomString(messageLength));
            jedis.incr("incr");
        }
        logger.info("[sendRandomMessage][ end ]{}", redisMeta.desc());
    }

    protected void sendRandomMessage(String ip, int port, int count, int messageLength) {
        Jedis jedis = new Jedis(ip, port);
        logger.info("[sendRandomMessage][begin]{}, {}", ip, port);
        for (int i = 0; i < count; i++) {

            long currentIndex = totalSendMessageCount.incrementAndGet();
            jedis.set(String.valueOf(currentIndex), randomString(messageLength));
            jedis.incr("incr");
        }
        logger.info("[sendRandomMessage][end]{}, {}", ip, port);
    }

    protected void sendMessage(RedisMeta redisMeta, int count, String message) {

        Jedis jedis = createJedis(redisMeta);
        logger.info("[sendMessage][begin]{}", redisMeta.desc());
        for (int i = 0; i < count; i++) {

            long currentIndex = totalSendMessageCount.incrementAndGet();
            jedis.set(String.valueOf(currentIndex), message);
        }
        logger.info("[sendMessage][ end ]{}", redisMeta.desc());
    }


    protected void executeCommands(String... args) throws ExecuteException, IOException {

        DefaultExecutor executor = new DefaultExecutor();
        executor.execute(CommandLine.parse(StringUtil.join(" ", args)));
    }

    protected void executeScript(String file, String... args) throws ExecuteException, IOException {

        URL url = getClass().getClassLoader().getResource(file);
        if (url == null) {
            url = getClass().getClassLoader().getResource("scripts/" + file);
            if (url == null) {
                throw new FileNotFoundException(file);
            }
        }
        DefaultExecutor executor = new DefaultExecutor();
        String command = "sh -v " + url.getFile() + " " + StringUtil.join(" ", args);
        executor.execute(CommandLine.parse(command));
    }

    protected SERVER_ROLE getRedisServerRole(RedisMeta slave) throws Exception {

        SimpleObjectPool<NettyClient> clientPool = NettyPoolUtil.createNettyPool(new DefaultEndPoint(slave.getIp(), slave.getPort()));
        String info = new InfoCommand(clientPool, "replication", scheduled).execute().get();
        for (String line : info.split("\r\n")) {
            String[] parts = line.split(":");
            if (parts.length >= 2 && parts[0].equals("role")) {
                String role = parts[1].trim();
                return SERVER_ROLE.of(role);
            }
        }
        return SERVER_ROLE.UNKNOWN;
    }

    protected XpipeMeta loadXpipeMeta(String configFile) throws SAXException, IOException {
        if (configFile == null) {
            return null;
        }
        InputStream ins = FileUtils.getFileInputStream(configFile, getClass());
        return DefaultSaxParser.parse(ins);
    }


    protected ClusterMeta getCluster(String dc, String clusterId) {

        DcMeta dcMeta = getDcMeta(dc);
        if (dcMeta == null) {
            return null;
        }
        ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);
        return clusterMeta;
    }

    protected ShardMeta getShard(String dc, String clusterId, String shardId) {

        ClusterMeta clusterMeta = getCluster(dc, clusterId);
        if (clusterMeta == null) {
            return null;
        }
        return clusterMeta.getShards().get(shardId);
    }


    protected List<RedisMeta> getRedises(String dc) {

        List<RedisMeta> result = new LinkedList<>();
        for (DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            if (!dcMeta.getId().equals(dc)) {
                continue;
            }
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    result.addAll(shardMeta.getRedises());
                }
            }
        }
        return result;
    }

    protected List<RedisMeta> getRedisSlaves(String dc) {

        List<RedisMeta> result = new LinkedList<>();

        for (RedisMeta redisMeta : getRedises(dc)) {
            if (!redisMeta.isMaster()) {
                result.add(redisMeta);
            }
        }
        return result;
    }

    protected DcMeta getDcMeta(String dc) {
        return getXpipeMeta().getDcs().get(dc);
    }

    protected List<KeeperMeta> getDcKeepers(String dc, String clusterId, String shardId) {
        return getXpipeMeta().getDcs().get(dc).getClusters().get(clusterId).getShards().get(shardId).getKeepers();

    }

    protected List<RedisMeta> getDcRedises(String dc, String clusterId, String shardId) {
        return getXpipeMeta().getDcs().get(dc).getClusters().get(clusterId).getShards().get(shardId).getRedises();

    }

    protected List<DcMeta> getDcMetas() {

        List<DcMeta> result = new LinkedList<>();

        for (DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            result.add(dcMeta);
        }
        return result;
    }

    protected RedisMeta getRedisMaster() {

        for (DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            List<RedisMeta> redises = getRedises(dcMeta.getId());
            for (RedisMeta redisMeta : redises) {
                if (redisMeta.isMaster()) {
                    return redisMeta;
                }
            }
        }
        return null;
    }


    protected DcMeta activeDc() {

        DcMeta activeDcMeta = null;

        for (DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            if (activeDcMeta != null) {
                break;
            }
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (activeDcMeta != null) {
                    break;
                }
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    if (activeDcMeta != null) {
                        break;
                    }
                    for (RedisMeta redisMeta : shardMeta.getRedises()) {
                        if (redisMeta.isMaster()) {
                            activeDcMeta = dcMeta;
                            break;
                        }
                    }
                }
            }
        }
        return activeDcMeta;
    }

    protected List<DcMeta> backupDcs() {

        List<DcMeta> result = new LinkedList<>();

        DcMeta active = activeDc();
        for (DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            if (!dcMeta.getId().equals(active.getId())) {
                result.add(dcMeta);
            }
        }
        return result;
    }

    protected String getInfoKey(RedisMeta slaveMeta, String infoSection, String key) throws Exception {

        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(slaveMeta.getIp(), slaveMeta.getPort()));
        String info = new InfoCommand(keyPool, infoSection, scheduled).execute().get();
        return new InfoResultExtractor(info).extract(key);
    }


    public XpipeMeta getXpipeMeta() {
        return xpipeMeta;
    }

    protected FakeRedisServer startFakeRedisServer() throws Exception {

        int port = randomPort(6379, 6479);
        return startFakeRedisServer(port);
    }

    protected FakeRedisServer startFakeRedisServer(int serverPort) throws Exception {

        FakeRedisServer fakeRedisServer = new FakeRedisServer(serverPort);
        fakeRedisServer.initialize();
        fakeRedisServer.start();
        add(fakeRedisServer);
        return fakeRedisServer;
    }


    protected String getAndSetDc(int index) {

        String dc = getDcMetas().get(index).getId();
        DefaultFoundationService.setDataCenter(dc);
        return dc;
    }

    protected KeeperMeta createNonExistKeeper(List<KeeperMeta> allKeepers) {

        Set<Integer> ports = new HashSet<>();
        for (KeeperMeta keeperMeta : allKeepers) {
            ports.add(keeperMeta.getPort());
        }

        int port = randomPort();
        while (true) {

            if (!ports.contains(port)) {
                break;
            }
            port = randomPort();
        }

        KeeperMeta result = new KeeperMeta();
        result.setPort(port).setIp("localhost");
        return result;
    }

    protected void changeClusterKeeper(ClusterMeta clusterMeta) {

        for (ShardMeta shardMeta : clusterMeta.getShards().values()) {

            KeeperMeta keeperMeta = shardMeta.getKeepers().get(0);
            keeperMeta.setPort(keeperMeta.getPort() + 10000);
        }
    }

    protected ClusterMeta differentCluster(String dc) {

        DcMeta dcMeta = getDcMeta(dc);
        ClusterMeta clusterMeta = (ClusterMeta) MetaClone.clone((ClusterMeta) dcMeta.getClusters().values().toArray()[0]);
        clusterMeta.setId(randomString(10));

        for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
            for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                keeperMeta.setPort(keeperMeta.getPort() + 10000);
            }
        }
        return clusterMeta;
    }

    protected KeeperMeta differentKeeper(List<KeeperMeta> keepers) {

        Set<Integer> ports = new HashSet<>();

        for (KeeperMeta keeper : keepers) {
            ports.add(keeper.getPort());
        }

        int port = randomPort();
        while (true) {
            if (!ports.contains(port)) {
                break;
            }
            port = randomPort();
        }

        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setId("localhost");
        keeperMeta.setPort(port);
        return keeperMeta;
    }

    protected List<KeeperMeta> createRandomKeepers(int count) {

        List<Integer> ports = new LinkedList<>(randomPorts(count));
        List<KeeperMeta> result = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            KeeperMeta keeperMeta = new KeeperMeta().setIp("localhost").setPort(ports.get(i));
            if (i == 0) {
                keeperMeta.setActive(true);
            } else {
                keeperMeta.setActive(false);
            }
            result.add(keeperMeta);
        }
        return result;
    }

    protected RedisMeta newRandomFakeRedisMeta() {
        return newRandomFakeRedisMeta("127.0.0.1", 6379);
    }

    protected RedisMeta newRandomFakeRedisMeta(String ip, int port) {
        DcMeta dcMeta = new DcMeta("dc");
        ClusterMeta clusterMeta = new ClusterMeta("cluster");
        clusterMeta.setActiveDc("dc").setType(ClusterType.ONE_WAY.toString());
        ShardMeta shardMeta = new ShardMeta("shard");
        RedisMeta redis = new RedisMeta().setIp(ip).setPort(port);
        shardMeta.addRedis(redis);
        clusterMeta.addShard(shardMeta);
        dcMeta.addCluster(clusterMeta);
        return redis;
    }
}
