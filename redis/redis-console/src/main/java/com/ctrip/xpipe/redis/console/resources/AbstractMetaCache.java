package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.exception.LoadConsoleMetaException;
import com.ctrip.xpipe.redis.console.exception.TooManyClustersRemovedException;
import com.ctrip.xpipe.redis.console.exception.TooManyDcsRemovedException;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.IpUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.cluster.ClusterType.HETERO;


/**
 * @author lishanglin
 * date 2021/3/16
 */
public abstract class AbstractMetaCache implements MetaCache {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    public static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    protected Pair<XpipeMeta, XpipeMetaManager> meta;

    protected Map<String, Triple<String, String, Long>> monitor2ClusterShard;

    /**
     * KeeperContainer 按 {@code keepercontainer_id} 建索引，随 Meta 一同换新。
     * {@code keepercontainer_id} 是 DB 自增主键，全局唯一，故不按 dc 分层。
     */
    protected volatile Map<Long, KeeperContainerMeta> keeperContainerById = Collections.emptyMap();

    protected long lastUpdateTime = 0;

    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    /**
     * 容器 Keeper 地址未解析出来，name 为容器 hostname。
     * Console 侧（hostname 反查不到实时 IP）与 Checker 侧（补不到所属 KeeperContainer 的 IP）共用，
     * 两者是同一条链路的上下游，合并观测即可。
     */
    private static final String KEEPER_IP_UNRESOLVED_EVENT = "keepercontainer.ip.unresolved";

    @Override
    public XpipeMeta getXpipeMeta() {
        try {
            return meta.getKey();
        } catch (Exception e) {
            logger.debug("[getXpipeMeta]", e);
        }
        return null;
    }

    @Override
    public XpipeMeta getDividedXpipeMeta(int partIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XpipeMetaManager.MetaDesc findMetaDesc(HostPort hostPort) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return xpipeMetaManager.findMetaDesc(hostPort);
    }

    @Override
    public String getXmlFormatDividedXpipeMeta(int partIndex) {
        throw new UnsupportedOperationException();
    }

    protected void refreshMeta(XpipeMeta xpipeMeta) {
        Pair<XpipeMeta, XpipeMetaManager> meta = new Pair<>(xpipeMeta, new DefaultXpipeMetaManager(xpipeMeta));
        // 先于 meta 发布：索引建好再换引用，读到新 meta 时索引必然已就绪
        keeperContainerById = indexKeeperContainers(xpipeMeta);
        AbstractMetaCache.this.meta = meta;
        monitor2ClusterShard = Maps.newHashMap();
        lastUpdateTime = System.currentTimeMillis();
    }

    private Map<Long, KeeperContainerMeta> indexKeeperContainers(XpipeMeta xpipeMeta) {
        if (null == xpipeMeta || null == xpipeMeta.getDcs()) {
            return Collections.emptyMap();
        }
        Map<Long, KeeperContainerMeta> index = new HashMap<>();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (null == dcMeta.getKeeperContainers()) {
                continue;
            }
            for (KeeperContainerMeta containerMeta : dcMeta.getKeeperContainers()) {
                if (null != containerMeta.getId()) {
                    index.put(containerMeta.getId(), containerMeta);
                }
            }
        }
        return index;
    }

    /**
     * 存入本地缓存前，把容器 Keeper 的 IP 从其所属 KeeperContainer 补齐 —— Console 已在 Meta 里填好
     * 容器的实时 IP（见 {@link #fillContainerKeeperIp}），Keeper 自身的 {@code redis_ip} 仍是占位值。
     *
     * <p>按 {@code keepercontainer_id} 关联，不用 IP：容器 Keeper 与其容器的 IP 都是占位
     * {@code 0.0.0.0}，以 IP 为键会把所有 pod 归并成一台机器。
     *
     * <p>容器形态只看 Keeper 的 {@code hostname} 是否有值；物理机 hostname 为空，IP 原样保留。
     * 补不到地址的容器 Keeper 保留占位 IP 并打点，便于量化这类实例。
     */
    protected void resolveContainerKeeperIp(XpipeMeta xpipeMeta) {
        if (null == xpipeMeta || null == xpipeMeta.getDcs()) {
            return;
        }

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            Map<Long, String> containerIpById = containerIpById(dcMeta);
            // 一个容器上可承载多个 Keeper，补不到时按 hostname 去重，避免同一容器刷出成片打点
            Set<String> unresolved = new LinkedHashSet<>();
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    for (KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                        resolveContainerKeeperIp(dcMeta, clusterMeta, shardMeta, keeperMeta, containerIpById, unresolved);
                    }
                }
            }
            unresolved.forEach(hostname ->
                    EventMonitor.DEFAULT.logEvent(KEEPER_IP_UNRESOLVED_EVENT, hostname));
        }
    }

    private Map<Long, String> containerIpById(DcMeta dcMeta) {
        Map<Long, String> containerIpById = new HashMap<>();
        if (null == dcMeta.getKeeperContainers()) {
            return containerIpById;
        }
        for (KeeperContainerMeta containerMeta : dcMeta.getKeeperContainers()) {
            if (null != containerMeta.getId()) {
                containerIpById.put(containerMeta.getId(), containerMeta.getIp());
            }
        }
        return containerIpById;
    }

    private void resolveContainerKeeperIp(DcMeta dcMeta, ClusterMeta clusterMeta, ShardMeta shardMeta,
                                          KeeperMeta keeperMeta, Map<Long, String> containerIpById,
                                          Set<String> unresolved) {
        if (StringUtil.isEmpty(keeperMeta.getHostname())) {
            return;
        }

        String ip = containerIpById.get(keeperMeta.getKeeperContainerId());
        if (StringUtil.isEmpty(ip) || XPipeConsoleConstant.DEFAULT_IP.equals(ip)) {
            // 日志保留 cluster/shard/port 等定位信息；打点只用 hostname，端口对同一容器恒定
            logger.warn("[resolveContainerKeeperIp][{}][{}][{}] unresolved keeper {}:{}, container {}, ip {}",
                    dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), keeperMeta.getHostname(),
                    keeperMeta.getPort(), keeperMeta.getKeeperContainerId(), ip);
            unresolved.add(keeperMeta.getHostname());
            return;
        }
        keeperMeta.setIp(ip);
    }

    protected XpipeMeta createXpipeMeta(List<DcMeta> dcMetas, List<RedisCheckRuleMeta> redisCheckRuleMetas){

        XpipeMeta xpipeMeta = new XpipeMeta();
        for (DcMeta dcMeta : dcMetas) {
            fillContainerKeeperIp(dcMeta);
            xpipeMeta.addDc(dcMeta);
        }

        setActiveDcForCrossDcClusters(xpipeMeta);

        for(RedisCheckRuleMeta redisCheckRuleMeta : redisCheckRuleMetas) {
            xpipeMeta.addRedisCheckRule(redisCheckRuleMeta);
        }
        xpipeMeta.setVersion(System.currentTimeMillis());
        return xpipeMeta;
    }

    /**
     * 对整份 Meta 按机房填充容器 KeeperContainer 的实时 IP，供 Meta 不是本地构建的部署形态使用
     * （如无 DB 的 Console：Meta 从有 DB 的 Console 拉取，但本机房 ZK 由自己监听，地址以本地为准）。
     *
     * <p>调用方须保证传入的 Meta 尚未发布（如刚反序列化出来的对象）。
     */
    protected void fillContainerKeeperIp(XpipeMeta xpipeMeta) {
        if (null == xpipeMeta || null == xpipeMeta.getDcs()) {
            return;
        }
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            fillContainerKeeperIp(dcMeta);
        }
    }

    /**
     * 把容器 KeeperContainer 的实时 IP 就地写入 {@link KeeperContainerMeta#setIp}，使下发给 Checker
     * 的 Meta 自带可直连地址，Checker 不必各自反查 ZK。
     *
     * <p>就地改写是安全的：每轮 {@code loadCache} 都重建 DcMeta（从 DB 构建或从上游 Console 反序列化），
     * 这里拿到的对象还未发布。
     *
     * <p>本地查不到时保留原 IP：Meta 若来自上游 Console，其中可能已带有上游填好的地址；
     * 只有最终仍为占位值的容器才计为未解析。
     *
     * <p>容器形态只看 {@code hostname} 是否有值 —— 与 {@code MetaUtils.theSame}、
     * {@code DefaultKeeperAdvancedService.isPodContainer} 口径一致；物理机 hostname 为空，IP 原样保留。
     *
     * <p>best-effort：远端机房读快照，console 首次刷新完成前查不到，失败的容器保留占位 IP
     * （DB 默认 {@code 0.0.0.0}），因此消费方仍须容忍占位值。
     *
     * <p>先按 hostname 筛出该机房的容器 KeeperContainer：没有容器就不必取地址缓存（纯物理机机房的
     * 常态）；有容器却取不到缓存才是异常，必须打点暴露，不能静默跳过。
     */
    private void fillContainerKeeperIp(DcMeta dcMeta) {
        if (null == dcMeta || null == dcMeta.getKeeperContainers() || dcMeta.getKeeperContainers().isEmpty()) {
            return;
        }

        List<KeeperContainerMeta> containers = new ArrayList<>();
        for (KeeperContainerMeta containerMeta : dcMeta.getKeeperContainers()) {
            if (!StringUtil.isEmpty(containerMeta.getHostname())) {
                containers.add(containerMeta);
            }
        }
        if (containers.isEmpty()) {
            return;
        }

        // 同一 hostname 可对应多条 keepercontainer 记录（端口不同），去重后再打点
        Set<String> unresolved = new LinkedHashSet<>();

        CrossDcKeeperContainerAddressService addressService = keeperContainerAddressService();
        Map<String, String> hostnameToIp = null == addressService
                ? Collections.emptyMap()
                : resolveHostnameToIp(addressService, dcMeta.getId());
        if (hostnameToIp.isEmpty()) {
            // 地址服务缺失或该机房缓存未就绪：这批容器一个都填不上，全量上报而非静默返回
            logger.warn("[fillContainerKeeperIp][{}] no address cache, {} container keepers",
                    dcMeta.getId(), containers.size());
            containers.stream().filter(this::isPlaceholderIp)
                    .forEach(containerMeta -> unresolved.add(containerMeta.getHostname()));
            reportUnresolvedContainers(dcMeta.getId(), unresolved);
            return;
        }

        for (KeeperContainerMeta containerMeta : containers) {
            String ip = hostnameToIp.get(containerMeta.getHostname());
            if (StringUtil.isEmpty(ip)) {
                if (isPlaceholderIp(containerMeta)) {
                    unresolved.add(containerMeta.getHostname());
                }
                continue;
            }
            containerMeta.setIp(ip);
        }

        reportUnresolvedContainers(dcMeta.getId(), unresolved);
    }

    private boolean isPlaceholderIp(KeeperContainerMeta containerMeta) {
        String ip = containerMeta.getIp();
        return StringUtil.isEmpty(ip) || XPipeConsoleConstant.DEFAULT_IP.equals(ip);
    }

    private void reportUnresolvedContainers(String dcId, Set<String> unresolved) {
        if (unresolved.isEmpty()) {
            return;
        }
        logger.warn("[fillContainerKeeperIp][{}] unresolved keeper containers: {}", dcId, unresolved);
        unresolved.forEach(hostname -> EventMonitor.DEFAULT.logEvent(KEEPER_IP_UNRESOLVED_EVENT, hostname));
    }

    /** 解析失败不得使整轮 Meta 加载失败：降级为空表，容器保留占位 IP。 */
    private Map<String, String> resolveHostnameToIp(CrossDcKeeperContainerAddressService addressService, String dcId) {
        try {
            Map<String, String> map = addressService.getAllHostnameToIp(dcId);
            return null == map ? Collections.emptyMap() : map;
        } catch (Throwable t) {
            logger.error("[fillContainerKeeperIp][{}] failed to resolve hostname to ip", dcId, t);
            return Collections.emptyMap();
        }
    }

    /**
     * 子类按部署形态提供地址来源；无地址服务时返回 null，填充整体跳过
     * （如无 DB 的 Checker 侧缓存，其 Meta 由 Console 下发时已填好）。
     */
    protected CrossDcKeeperContainerAddressService keeperContainerAddressService() {
        return null;
    }

    @Override
    public XpipeMeta getXpipeMetaLongPull(long version) throws InterruptedException {
        return null;
    }

    void setActiveDcForCrossDcClusters(XpipeMeta xpipeMeta) {
        try {
            Map<String, Map<String, ClusterMeta>> crossDcClusters = typeDcClusterMap(xpipeMeta, ClusterType.CROSS_DC);

            if (crossDcClusters.isEmpty())
                return;

            crossDcClusters.values().forEach(dcClusters -> {
                Map<String, Integer> dcMasterNums = countDcMaster(dcClusters);
                String activeDc = maxMasterCountDc(dcMasterNums);
                dcClusters.values().forEach(dcCluster -> dcCluster.setActiveDc(activeDc));
            });

        } catch (Throwable e) {
            logger.error("[setActiveDcForCrossDcClusters]", e);
        }
    }

    Map<String, Integer> countDcMaster(Map<String, ClusterMeta> dcClusters) {
        Map<String, Integer> dcMasterCountMap = new HashMap<>();
        dcClusters.forEach((dc, clusterMeta) -> {
            dcMasterCountMap.put(dc, dcMasterCount(clusterMeta));
        });
        return dcMasterCountMap;
    }

    Map<String, Map<String, ClusterMeta>> typeDcClusterMap(XpipeMeta xpipeMeta, ClusterType clusterType) {
        Map<String, Map<String, ClusterMeta>> typeDcClusterMetaMap = new HashMap<>();
        xpipeMeta.getDcs().forEach((dc, dcMeta) -> {
            dcMeta.getClusters().forEach((clusterName, clusterMeta) -> {
                if (ClusterType.lookup(clusterMeta.getType()).equals(clusterType)) {
                    typeDcClusterMetaMap.putIfAbsent(clusterName, new HashMap<>());
                    typeDcClusterMetaMap.get(clusterName).put(dc, clusterMeta.setActiveDc(""));
                }
            });
        });
        return typeDcClusterMetaMap;
    }

    String maxMasterCountDc(Map<String, Integer> dcMasterNumMap) {
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>(dcMasterNumMap.entrySet());
        entryList.sort(new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return entryList.get(0).getKey();
    }

    private int dcMasterCount(ClusterMeta dcCluster) {
        Map<String, ShardMeta> shards = dcCluster.getShards();
        AtomicInteger masterCount = new AtomicInteger();
        shards.forEach((shardId, shardMeta) -> {
            shardMeta.getRedises().forEach(redisMeta -> {
                if (redisMeta.isMaster()) {
                    masterCount.incrementAndGet();
                }
            });
        });
        return masterCount.get();
    }

    /**
     * 分片 Meta 只按 cluster 分片：KeeperContainer 与 route / sentinel 等 dc 级配置一样整份携带。
     * Checker 的 TFS 判定是 {@code KeeperMeta.keeperContainerId} → 同 DcMeta 的 KeeperContainer 点查，
     * 缺少 container 只会静默退化，不能按 part 过滤（spec keeper-tfs-m5 D47）。
     */
    protected XpipeMeta createDividedMeta(XpipeMeta full, Set<String> reqClusters) {
        XpipeMeta part = new XpipeMeta();
        for (DcMeta dcMeta: full.getDcs().values()) {
            DcMeta partDcMeta = new DcMeta(dcMeta.getId()).setLastModifiedTime(dcMeta.getLastModifiedTime()).setZone(dcMeta.getZone());
            part.addDc(partDcMeta);

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (!reqClusters.contains(clusterMeta.getId())) {
                    continue;
                }

                ClusterMeta partClusterMeta = new ClusterMeta(clusterMeta.getId());
                partClusterMeta.mergeAttributes(clusterMeta);
                clusterMeta.getSources().forEach(partClusterMeta::addSource);
                clusterMeta.getShards().forEach((k, v) -> partClusterMeta.addShard(v));

                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (clusterType == ClusterType.HETERO) {
                    String activeDc = clusterMeta.getActiveDc();
                    String[] backupDcs = clusterMeta.getBackupDcs().split("\\s*,\\s*");
                    Set<String> dcs = new HashSet<>();
                    dcs.add(activeDc);
                    dcs.addAll(Arrays.asList(backupDcs));

                    if (dcs.contains(currentDc)) {
                        ClusterType azGroupType = ClusterType.lookup(clusterMeta.getAzGroupType());
                        partClusterMeta.setType(azGroupType.toString());
                        partClusterMeta.setAzGroupType(null);
                    } else {
                        continue;
                    }
                }

                partDcMeta.addCluster(partClusterMeta);
            }
            dcMeta.getSentinels().values().forEach(partDcMeta::addSentinel);
            dcMeta.getKeeperContainers().forEach(partDcMeta::addKeeperContainer);
            dcMeta.getRoutes().forEach(partDcMeta::addRoute);
            dcMeta.getMetaServers().forEach(partDcMeta::addMetaServer);
        }
        full.getRedisCheckRules().values().forEach(part::addRedisCheckRule);

        return part;
    }

    @Override
    public boolean inBackupDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String instanceInDc = metaDesc.getDcId();
        String activeDc = metaDesc.getActiveDc();
        String azGroupType = metaDesc.getAzGroupType();
        ClusterType azGroupClusterType = StringUtil.isEmpty(azGroupType)
            ? null : ClusterType.lookup(azGroupType);
        return !activeDc.equalsIgnoreCase(instanceInDc) && (azGroupClusterType != ClusterType.SINGLE_DC);
    }

    @Override
    public HostPort findMasterInSameShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }

        String clusterName = metaDesc.getClusterId();
        String shardName = metaDesc.getShardId();

        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterName, shardName);
        // could be null if no master in a shard
        if(redisMaster == null) {
            return null;
        }
        RedisMeta redisMeta = redisMaster.getValue();
        return new HostPort(redisMeta.getIp(), redisMeta.getPort());
    }

    @Override
    public Pair<String, String> findClusterShard(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        return new Pair<>(metaDesc.getClusterId(), metaDesc.getShardId());
    }

    @Override
    public String getActiveDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        return metaDesc.getActiveDc();
    }

    @Override
    public ClusterType getAzGroupType(HostPort hostPort) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();

        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);
        if (metaDesc == null) {
            return null;
        }

        String azGroupType = metaDesc.getAzGroupType();
        return StringUtil.isEmpty(azGroupType)
            ? null : ClusterType.lookup(azGroupType);
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getVersion() {
        if(getXpipeMeta() != null) {
            return getXpipeMeta().getVersion();
        } else {
            return 0l;
        }
    }


    @Override
    public List<RouteMeta> getCurrentDcConsoleRoutes() {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return xpipeMetaManager.consoleRoutes(CURRENT_IDC);
    }

    @Override
    public boolean isCrossRegion(String activeDc, String backupDc) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return !xpipeMetaManager.getDcZone(activeDc)
                .equalsIgnoreCase(xpipeMetaManager.getDcZone(backupDc));
    }

    @Override
    public String getDcZone(String dc) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return xpipeMetaManager.getDcZone(dc);
    }

    @Override
    public boolean isCurrentDc(String dc) {
        return currentDc.equalsIgnoreCase(dc);
    }

    @Override
    public boolean isDcInRegion(String dc, String zone) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        return xpipeMetaManager.getDcZone(dc).equalsIgnoreCase(zone);
    }

    @Override
    public List<HostPort> getAllActiveRedisOfDc(String activeDc, String dcId) {
        List<HostPort> result = Lists.newLinkedList();
        boolean isDcActiveDc = activeDc.equalsIgnoreCase(dcId);
        try {
            for(ClusterMeta clusterMeta : meta.getKey().findDc(dcId).getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (clusterType.supportSingleActiveDC() && !clusterMeta.getActiveDc().equalsIgnoreCase(activeDc)) {
                    continue;
                }
                if (clusterType.supportMultiActiveDC() && !isDcActiveDc) {
                    continue;
                }
                for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    for(RedisMeta redis : shardMeta.getRedises()) {
                        result.add(new HostPort(redis.getIp(), redis.getPort()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[getRedisNumOfDc]", e);
        }
        return result;
    }

    @Override
    public String getSentinelMonitorName(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId);
        return xpipeMetaManager.getSentinelMonitorName(activeDc, clusterId, shardId);
    }

    @Override
    public Set<HostPort> getActiveDcSentinels(String clusterId, String shardId) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();

        String activeDc = xpipeMetaManager.getActiveDc(clusterId);
        SentinelMeta sentinel = xpipeMetaManager.getSentinel(activeDc, clusterId, shardId);

        return new HashSet<>(IpUtils.parseAsHostPorts(sentinel.getAddress()));
    }

    @Override
    public Set<HostPort> getAllSentinels() {
        Set<HostPort> sentinels = new HashSet<>();
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        xpipeMetaManager.getAllSentinels().forEach(sentinelMeta -> sentinels.addAll(IpUtils.parseAsHostPorts(sentinelMeta.getAddress())));
        return sentinels;
    }

    @Override
    public HostPort findMaster(String clusterId, String shardId) throws MasterNotFoundException {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        Pair<String, RedisMeta> redisMaster = xpipeMetaManager.getRedisMaster(clusterId, shardId);
        if (redisMaster == null) {
            throw new MasterNotFoundException(clusterId, shardId);
        }
        return new HostPort(redisMaster.getValue().getIp(), redisMaster.getValue().getPort());
    }

    @Override
    public List<RedisMeta> getRedisOfDcClusterShard(String dc, String cluster, String shard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
        if (null == shardMeta) return Collections.emptyList();
        return shardMeta.getRedises();
    }

    @Override
    public List<KeeperMeta> getKeeperOfDcClusterShard(String dc, String cluster, String shard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
        if (null == shardMeta) return Collections.emptyList();
        return shardMeta.getKeepers();
    }

    /** 直接走缓存里的 Meta 对象树，不经 {@code doGetShardMeta} 的深拷贝。 */
    @Override
    public List<KeeperMeta> getKeeperOfDcClusterShardRef(String dc, String cluster, String shard) {
        XpipeMeta xpipeMeta = getXpipeMeta();
        if (null == xpipeMeta || null == dc || null == cluster || null == shard) {
            return Collections.emptyList();
        }
        DcMeta dcMeta = xpipeMeta.findDc(dc);
        if (null == dcMeta) {
            return Collections.emptyList();
        }
        ClusterMeta clusterMeta = dcMeta.findCluster(cluster);
        if (null == clusterMeta) {
            return Collections.emptyList();
        }
        ShardMeta shardMeta = clusterMeta.findShard(shard);
        if (null == shardMeta || null == shardMeta.getKeepers()) {
            return Collections.emptyList();
        }
        return shardMeta.getKeepers();
    }

    /**
     * {@code doGetKeeperContainer} 在 dc / container 不存在时抛 {@link IllegalArgumentException}，
     * 这里按「查不到」处理：调用方（打点）不应因 meta 不全而失败。
     */
    @Override
    public String getKeeperDiskType(KeeperMeta keeperMeta) {
        if (null == keeperMeta || null == keeperMeta.getKeeperContainerId()) {
            return null;
        }
        // 走随 Meta 预建的 id 索引：避免 doGetKeeperContainer 的线性扫描与整对象深拷贝 ——
        // 该方法在每个跨机房 redis 的延迟打点上都会被调用
        KeeperContainerMeta containerMeta = keeperContainerById.get(keeperMeta.getKeeperContainerId());
        return null == containerMeta ? null : containerMeta.getDiskType();
    }

    @Override
    public Map<KeeperMeta, String> getKeeper2DcOfClusterShard(String cluster, String shard) {
        Map<KeeperMeta, String> keepers = new HashMap<>();
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        xpipeMetaManager.doGetDcs().forEach(dc -> {
            ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
            if (shardMeta != null) {
                shardMeta.getKeepers().forEach(keeperMeta -> {
                    keepers.put(keeperMeta, dc);
                });
            }
        });
        return keepers;
    }

    @Override
    public List<RedisMeta> getSlavesOfDcClusterShard(String dc, String cluster, String shard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
        if (null == shardMeta) return Collections.emptyList();
        return shardMeta.getRedises().stream().filter(redisMeta -> !redisMeta.isMaster()).collect(Collectors.toList());
    }

    @Override
    public List<RedisMeta> getSlavesOfShard(String cluster, String shard) {
        List<RedisMeta> slaves = new ArrayList<>();
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        xpipeMetaManager.doGetDcs().forEach(dc -> {
            ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
            if (shardMeta != null) {
                shardMeta.getRedises().forEach(redisMeta -> {
                    if (!redisMeta.isMaster())
                        slaves.add(redisMeta);
                });
            }
        });
        return slaves;
    }

    @Override
    public List<RedisMeta> getAllInstancesOfShard(String cluster, String shard) {
        List<RedisMeta> instances = new ArrayList<>();
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        xpipeMetaManager.doGetDcs().forEach(dc -> {
            ShardMeta shardMeta = xpipeMetaManager.doGetShardMeta(dc, cluster, shard);
            if (shardMeta != null) {
                instances.addAll(shardMeta.getRedises());
            }
        });
        return instances;
    }

    @Override
    public List<RedisMeta> getAllInstanceOfDc(String cluster, String dc) {
        return meta.getValue().getRedises(dc, cluster);
    }

    @Override
    public Map<String, List<RedisMeta>> getAllInstance(String cluster) {
        return meta.getValue().getRedises(cluster);
    }

    @Override
    public String getDc(HostPort hostPort) {

        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc metaDesc = xpipeMetaManager.findMetaDesc(hostPort);

        if (metaDesc == null) {
            throw new IllegalStateException("unfound shard for instance:" + hostPort);
        }
        return metaDesc.getDcId();
    }

    @Override
    public String getDcByIpAndClusterShard(String hostIp, Pair<String, String> clusterShard) {
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        Set<String> relatedDcs = xpipeMetaManager.getRelatedDcs(clusterShard.getKey(), clusterShard.getValue());

        for (String dc : relatedDcs) {
            for (RedisMeta redis : xpipeMetaManager.getRedises(dc, clusterShard.getKey(), clusterShard.getValue())) {
                if (redis.getIp().equalsIgnoreCase(hostIp)) {
                   return dc;
                }
            }
        }
        return null;
    }

    @Override
    public Triple<String, String, Long> findClusterShardBySentinelMonitor(String monitor) {
        if(StringUtil.isEmpty(monitor)) {
            return null;
        }

        Triple<String, String, Long> clusterShard = monitor2ClusterShard.get(monitor);
        if(clusterShard != null) {
            return clusterShard;
        }

        synchronized (this) {
            loadSentinelMonitorInfo();
        }
        return monitor2ClusterShard.get(monitor);
    }

    private void loadSentinelMonitorInfo() {
        try {
            XpipeMeta xpipeMeta = meta.getKey();
            for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
                for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                    ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                    ClusterType azGroupType = StringUtil.isEmpty(clusterMeta.getAzGroupType())
                        ? null : ClusterType.lookup(clusterMeta.getAzGroupType());
                    if (clusterType.supportSingleActiveDC() && azGroupType != ClusterType.SINGLE_DC
                        && !clusterMeta.getActiveDc().equals(dcMeta.getId())) {
                        continue;
                    }

                    for (ShardMeta shardMeta : clusterMeta.getShards().values()) {
                        monitor2ClusterShard.put(shardMeta.getSentinelMonitorName(),
                                new Triple(clusterMeta.getId(), shardMeta.getId(), shardMeta.getSentinelId()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[loadSentinelMonitorInfo]", e);
            throw e;
        }
    }

    @Override
    public String getActiveDc(String clusterId){
        XpipeMetaManager xpipeMetaManager  =  meta.getValue();
        return xpipeMetaManager.getActiveDc(clusterId);
    }

    @Override
    public ClusterType getClusterType(String clusterId) {
        XpipeMeta xpipeMeta = meta.getKey();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!dcMeta.getClusters().containsKey(clusterId)) continue;
            return ClusterType.lookup(dcMeta.getClusters().get(clusterId).getType());
        }

        throw new IllegalStateException("[getClusterType] unfound cluster for name:" + clusterId);
    }

    @Override
    public boolean isMetaChain(HostPort chainSrc, HostPort chainDst) {
        Pair<String,String> clusterShard = findClusterShard(chainDst);
        ClusterType clusterType = getClusterType(clusterShard.getKey());

        // currently chainSrc contains random port, which can't be used to validate if chainSrc
        // is a keeper (for ONE_WAY) or a redis (for BI_DIRECTION).
        // TODO: check chainSrc if keeper & redis FORWARD_FOR their listening port
        XpipeMetaManager xpipeMetaManager = meta.getValue();
        XpipeMetaManager.MetaDesc dstDesc = xpipeMetaManager.findMetaDesc(chainDst);
        if (dstDesc == null) return false;
        Redis redis = dstDesc.getRedis();
        if (redis == null) return false;

        if ((clusterType.equals(ClusterType.ONE_WAY) && (redis instanceof KeeperMeta)) ||
                (clusterType.equals(ClusterType.BI_DIRECTION) && (redis instanceof RedisMeta))) {
            return true;
        }
        return false;
    }

    @Override
    public Map<Long, String> dcShardIds(String clusterId, String dcId) {
        DcMeta dcMeta = meta.getKey().findDc(dcId);
        if (dcMeta == null) return new HashMap<>();
        ClusterMeta clusterMeta = dcMeta.findCluster(clusterId);
        if (clusterMeta == null) return new HashMap<>();
        return clusterMeta.getShards().values().stream().collect(Collectors.toMap(ShardMeta::getDbId, ShardMeta::getId));
    }

    protected void checkMeta(XpipeMeta future, int maxRemovedDcs, int maxRemovedClusterPercent) {
        if (meta == null) return;
        if (future == null) throw new LoadConsoleMetaException("xpipe meta from console is null");

        checkDcsCnt(meta.getKey(), future, maxRemovedDcs);

        for (DcMeta currentDcMeta : meta.getKey().getDcs().values()) {
            DcMeta futureDcMeta = future.findDc(currentDcMeta.getId());
            checkDcClustersCnt(currentDcMeta, futureDcMeta, maxRemovedClusterPercent);
        }
    }

    private void checkDcsCnt(XpipeMeta current, XpipeMeta future, int maxRemovedDcs) {
        int currentDcCount = current.getDcs().size();
        int futureDcCount = future.getDcs().size();
        if (currentDcCount - futureDcCount > maxRemovedDcs)
            throw new TooManyDcsRemovedException(String.format("current dcs:%s,future dcs:%s", current.getDcs().keySet(), future.getDcs().keySet()));
    }

    private void checkDcClustersCnt(DcMeta currentDcMeta, DcMeta futureDcMeta, int maxRemovedClusterPercent) {
        if (futureDcMeta == null) return;

        int currentClusterCount = currentDcMeta.getClusters().size();
        int futureClusterCount = futureDcMeta.getClusters().size();
        int clusterRemoved = currentClusterCount - futureClusterCount;
        if ((clusterRemoved) > currentClusterCount * maxRemovedClusterPercent / 100 && clusterRemoved > 5)
            throw new TooManyClustersRemovedException(String.format("dc:%s, current cluster count:%d,future cluster count:%d", currentDcMeta.getId(), currentClusterCount, futureClusterCount));
    }

    @Override
    public Map<String, Map<String, Integer>> getClusterShardCounts() {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        XpipeMeta xpipeMeta = meta.getKey();
        if (xpipeMeta == null) return result;
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                result.computeIfAbsent(clusterMeta.getId(), k -> new HashMap<>())
                        .put(dcMeta.getId(), clusterMeta.getShards().size());
            }
        }
        return result;
    }

    @Override
    public Set<String> getAllShardNamesByClusterName(String clusterName) {
        Set<String> shards = new HashSet<>();
        XpipeMeta xpipeMeta = meta.getKey();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
            if (clusterMeta != null) {
               Map<String, ShardMeta> shardMetaMap = clusterMeta.getShards();
               shards.addAll(shardMetaMap.keySet());
            }
        }
        return shards;
    }

    @Override
    public Map<String, Integer> getClusterCntMap(String clusterName) {
        Map<String, Integer> clusterCntMap = new HashMap<>();
        for (DcMeta dcMeta : meta.getKey().getDcs().values()) {
            ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
            if(clusterMeta == null) {
                continue;
            }
            int cnt = 0;
            for(ShardMeta shardMeta : clusterMeta.getShards().values()) {
                cnt += shardMeta.getRedises().size();
            }
            clusterCntMap.put(dcMeta.getId(), cnt);
        }
        return clusterCntMap;
    }

    @Override
    public boolean isDcClusterMigratable(String clusterName, String dc) {
        if (StringUtil.isEmpty(clusterName) || StringUtil.isEmpty(dc)) return false;
        DcMeta dcMeta = meta.getKey().getDcs().get(dc);
        if (dcMeta == null) return false;
        ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
        if (clusterMeta == null) return false;
        return ClusterType.supportClusterMigration(clusterMeta.getType(), clusterMeta.getAzGroupType());
    }

    @Override
    public boolean anyDcMigratable(String clusterName) {
        if (StringUtil.isEmpty(clusterName)) return false;
        for (DcMeta dcMeta : meta.getKey().getDcs().values()) {
            ClusterMeta clusterMeta = dcMeta.findCluster(clusterName);
            if (clusterMeta == null) continue;
            ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
            if ((clusterType != HETERO && clusterType.supportMigration())
                    || (!StringUtil.isEmpty(clusterMeta.getAzGroupType()) && ClusterType.lookup(clusterMeta.getAzGroupType()).supportMigration())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getMigratableClustersCountByActiveDc(String dc) {
        if (StringUtil.isEmpty(dc)) return 0;
        DcMeta dcMeta = meta.getKey().findDc(dc);
        if (null == dcMeta) return 0;

        int cnt = 0;
        for (ClusterMeta cluster: dcMeta.getClusters().values()) {
            if (!dc.equalsIgnoreCase(cluster.getActiveDc())) continue;
            ClusterType type = ClusterType.lookup(cluster.getType());
            if (type.equals(HETERO)) {
                type = ClusterType.lookup(cluster.getAzGroupType());
            }
            if (type.supportMigration()) cnt++;
        }
        return cnt;
    }

    @Override
    public Map<String, Integer> getAllDcMigratableClustersCnt() {
        Map<String, Integer> ret = new HashMap<>();
        for (String dc: meta.getKey().getDcs().keySet()) {
            ret.put(dc, getMigratableClustersCountByActiveDc(dc));
        }
        return ret;
    }

    @Override
    public boolean isBackupDcAndCrossRegion(String currentDc, String activeDc, List<String> backupDcs) {
        if (activeDc == null || backupDcs == null || backupDcs.isEmpty()) return false;
        backupDcs = backupDcs.stream().map(String::toLowerCase).collect(Collectors.toList());
        return isCrossRegion(activeDc, currentDc) && backupDcs.contains(currentDc.toLowerCase());
    }

    @Override
    public List<String> currentRegionDcs() {
        if (meta == null)
            throw new LoadConsoleMetaException("xpipe meta from console is null");

        Map<String, DcMeta> dcs = meta.getKey().getDcs();
        DcMeta targetDcMeta = dcs.get(currentDc);
        if (null == targetDcMeta)
            return Collections.emptyList();

        String zone = targetDcMeta.getZone();
        List<String> regionDcs = new ArrayList<>();
        for (DcMeta dcMeta : dcs.values()) {
            if (dcMeta.getZone().equalsIgnoreCase(zone)) {
                regionDcs.add(dcMeta.getId());
            }
        }
        return regionDcs;
    }

    @VisibleForTesting
    public AbstractMetaCache setMeta(Pair<XpipeMeta, XpipeMetaManager> meta) {
        this.meta = meta;
        return this;
    }

    @VisibleForTesting
    public AbstractMetaCache setMonitor2ClusterShard(Map<String, Triple<String, String, Long>> monitorMap) {
        this.monitor2ClusterShard = monitorMap;
        return this;
    }

}
