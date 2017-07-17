package com.ctrip.xpipe.redis.integratedtest.tools;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import com.ctrip.xpipe.redis.meta.server.service.console.ConsoleServiceImpl;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestOperations;

import java.util.*;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 12, 2017
 */
public class ConfigCheck extends AbstractIntegratedTest {

    private String addr = System.getProperty("xpipe_addr", "http://127.0.0.1:8080");

    //fake address
    private String clientAddr = System.getProperty("credis_addr", "http://127.0.0.1:8090");

    private String[] dcs = new String[]{"SHAJQ", "SHAOY"};

    private String checkCluster = System.getProperty("cluster", "test");

    @Test
    public void check() {

        XpipeMeta xpipeMeta = getXpipeMetaFromConsole();

        logger.info("[check][cluster]{}", checkCluster);

        CRedisMeta cRedisMeta = getCRedisMeta(checkCluster);

        cRedisMeta.check();

        CheckCluster checkXpipe = fromXpipe(xpipeMeta, checkCluster);

        CheckCluster checkCredis = fromRedis(cRedisMeta, checkCluster);

        Assert.assertEquals(checkCredis, checkXpipe);

        logger.info("[check][successful]{}", checkCluster);
    }

    private CRedisMeta getCRedisMeta(String checkCluster) {

        RestOperations restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
        CRedisMeta result = restOperations.getForObject(clientAddr, CRedisMeta.class, checkCluster);
        logger.debug("{}", new JsonCodec(true).encode(result));
        return result;
    }

    private XpipeMeta getXpipeMetaFromConsole() {

        ConsoleService consoleService = new ConsoleServiceImpl();
        ((ConsoleServiceImpl) consoleService).setHost(addr);
        XpipeMeta xpipeMeta = new XpipeMeta();
        for (String dc : dcs) {
            DcMeta dcMeta = consoleService.getDcMeta(dc);
            xpipeMeta.addDc(dcMeta);
        }
        return xpipeMeta;
    }

    @Override
    protected List<RedisMeta> getRedisSlaves() {
        return null;
    }


    private CheckCluster fromRedis(CRedisMeta cRedisMeta, String checkCluster) {

        CheckCluster result = new CheckCluster(checkCluster);
        cRedisMeta.getGroups().forEach(groupMeta -> {

            CheckShard shard = result.getOrCreate(groupMeta.getName());
            groupMeta.getInstances().forEach(instance -> {
                CheckRedis checkRedis = new CheckRedis(instance.getIPAddress(), instance.getPort(), instance.getEnv());
                shard.addRedis(checkRedis);
            });
        });

        return result;
    }

    private CheckCluster fromXpipe(XpipeMeta xpipeMeta, String checkCluster) {

        XpipeMetaManager xpm = new DefaultXpipeMetaManager(xpipeMeta);
        CheckCluster result = new CheckCluster(checkCluster);

        for (String dc : dcs) {

            ClusterMeta clusterMeta = xpm.getClusterMeta(dc, checkCluster);
            if (clusterMeta == null) {
                continue;
            }
            for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                CheckShard orShard = result.getOrCreate(shardMeta.getId());
                shardMeta.getRedises().forEach(redis -> {
                    orShard.addRedis(new CheckRedis(redis.getIp(), redis.getPort(), dc));
                });
            }
        }
        return result;
    }

    public static class CheckCluster extends AbstractMeta {

        private String clusterName;
        private Map<String, CheckShard> shards = new HashMap<>();

        public CheckCluster(String clusterName) {
            this.clusterName = clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public CheckShard getOrCreate(String shardName) {
            return MapUtils.getOrCreate(shards, shardName, new ObjectFactory<CheckShard>() {
                @Override
                public CheckShard create() {
                    return new CheckShard(shardName);
                }
            });
        }

        public String getClusterName() {
            return clusterName;
        }

        public Map<String, CheckShard> getShards() {
            return shards;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CheckCluster)) {
                return false;
            }

            CheckCluster other = (CheckCluster) obj;
            if (!(ObjectUtils.equals(clusterName, other.clusterName))) {
                logger.info("[clusterName Not Equal]{}, {}", clusterName, other.clusterName);
                return false;
            }
            if (!(ObjectUtils.equals(shards, other.shards))) {
                logger.info("[shards not equal]{}", clusterName);
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(clusterName);
        }
    }

    public static class CheckShard extends AbstractMeta {

        private String shardName;
        private Set<CheckRedis> redises = new HashSet<>();

        public CheckShard(String shardName) {
            this.shardName = shardName;
        }

        public void setShardName(String shardName) {
            this.shardName = shardName;
        }

        public void addRedis(CheckRedis checkRedis) {
            redises.add(checkRedis);
        }

        public String getShardName() {
            return shardName;
        }

        public Set<CheckRedis> getRedises() {
            return redises;
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(shardName);
        }

        @Override
        public boolean equals(Object obj) {

            if (!(obj instanceof CheckShard)) {
                return false;
            }

            CheckShard other = (CheckShard) obj;
            if (!ObjectUtils.equals(shardName, other.shardName)) {
                logger.info("[shardName Not Equal]{}, {}", shardName, other.shardName);
                return false;
            }
            if (!ObjectUtils.equals(redises, other.redises)) {
                logger.info("[redises not equal]{}, \n{}, \n{}", shardName, redises, other.redises);
                return false;
            }
            return true;
        }
    }

    public static class CheckRedis extends AbstractMeta {

        private HostPort hostPort;
        private String idc;

        public CheckRedis(String host, int port, String idc) {
            this.hostPort = new HostPort(host, port);
            this.idc = idc;
        }

        public void setHostPort(HostPort hostPort) {
            this.hostPort = hostPort;
        }

        public void setIdc(String idc) {
            this.idc = idc;
        }

        public HostPort getHostPort() {
            return hostPort;
        }

        public String getIdc() {
            return idc;
        }

        @Override
        public boolean equals(Object obj) {

            if (!(obj instanceof CheckRedis)) {
                return false;
            }
            CheckRedis other = (CheckRedis) obj;
            if (!(ObjectUtils.equals(hostPort, other.hostPort))) {
                return false;
            }
            if (!(ObjectUtils.equals(idc, other.idc))) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(hostPort, idc);
        }

    }

    public static abstract class AbstractMeta {

        protected Logger logger = LoggerFactory.getLogger(getClass());

        @Override
        public String toString() {
            return JsonCodec.INSTANCE.encode(this);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CRedisMeta extends AbstractMeta {

        public static int READ_ACTIVE_SLAVES = 2;

        private boolean isXpipe;
        private String masterIDC;
        private String name;
        private int rule;
        private String ruleName;
        private boolean usingIdc;
        private List<GroupMeta> groups;

        public void check() {

            if (rule != READ_ACTIVE_SLAVES) {
                throw new IllegalStateException(String.format("[check][rule error]cluster:%s, rule:%s", name, ruleName));
            }
            if (groups != null) {
                groups.forEach(groupMeta -> groupMeta.check());

            }
        }

        public boolean isXpipe() {
            return isXpipe;
        }

        public void setIsXpipe(boolean xpipe) {
            isXpipe = xpipe;
        }

        public String getMasterIDC() {
            return masterIDC;
        }

        public void setMasterIDC(String masterIDC) {
            this.masterIDC = masterIDC;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getRule() {
            return rule;
        }

        public void setRule(int rule) {
            this.rule = rule;
        }

        public String getRuleName() {
            return ruleName;
        }

        public void setRuleName(String ruleName) {
            this.ruleName = ruleName;
        }

        public boolean isUsingIdc() {
            return usingIdc;
        }

        public void setUsingIdc(boolean usingIdc) {
            this.usingIdc = usingIdc;
        }

        public List<GroupMeta> getGroups() {
            return groups;
        }

        public void setGroups(List<GroupMeta> groups) {
            this.groups = groups;
        }

    }

    public static class GroupMeta extends AbstractMeta {
        private String name;
        private List<InstanceMeta> instances;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<InstanceMeta> getInstances() {
            return instances;
        }

        public void setInstances(List<InstanceMeta> instances) {
            this.instances = instances;
        }

        public void check() {
            if (instances != null) {
                instances.forEach(instanceMeta -> instanceMeta.check());
            }
        }
    }

    public static class InstanceMeta extends AbstractMeta {

        private boolean canRead;
        private String env;
        private String IPAddress;
        private boolean isMaster;
        private int port;
        private boolean status;

        public void check() {

            String addr = String.format("%s:%d", IPAddress, port);
            if (!canRead) {
                throw new IllegalStateException("instance can not read:" + addr);
            }
            if (!status) {
                throw new IllegalStateException("instance not valid:" + addr);
            }

        }


        public boolean isCanRead() {
            return canRead;
        }

        public void setCanRead(boolean canRead) {
            this.canRead = canRead;
        }

        public String getEnv() {
            return env;
        }

        public void setEnv(String env) {
            this.env = env;
        }

        public String getIPAddress() {
            return IPAddress;
        }

        public void setIPAddress(String IPAddress) {
            this.IPAddress = IPAddress;
        }

        public boolean isMaster() {
            return isMaster;
        }

        public void setIsMaster(boolean master) {
            isMaster = master;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public boolean isStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }
    }

    public static void main(String[] argc) {
        new ConfigCheck().check();
    }
}
