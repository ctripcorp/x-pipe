package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Map;

/**
 * 跨机房 keepercontainer hostname ↔ IP 查询门面（keeper-dynamic-ip spec §8.5）。
 * <p>
 * <b>只读内存、禁止 IO</b>：本机房读 {@code CurrentDcKeeperContainerAddressService}，远端机房读 console 周期拉取的
 * 快照缓存（{@code RemoteDcKeeperContainerAddressCache}），可在循环与周期任务中调用。
 * <p>
 * 查询规则与在目标机房本地调用一致：hostname → IP 先查存活、未命中再查未到期 tombstone；IP → hostname 只认存活。
 * <p>
 * 代价（D-J5）：远端结果最多滞后一个刷新周期；console 启动后首轮刷新完成前远端查询全部解析不到。
 * 拿到过期 IP 发直连写命令时由容器端 hostname 校验返回 421 挡住。
 */
public interface CrossDcKeeperContainerAddressService {

    /**
     * 解析指定机房某个 hostname 对应的 IP
     *
     * @param dcId     机房 ID，大小写不敏感
     * @param hostname keeper container hostname
     * @return IP；未找到（含远端机房尚无快照）返回 null
     */
    String getIpByHostname(String dcId, String hostname);

    /**
     * 获取指定机房所有 keeper container 的 hostname → IP 映射：存活正向 ∪ 未到期 tombstone，
     * 与逐个调用 {@link #getIpByHostname(String, String)} 结果一致
     *
     * @param dcId 机房 ID，大小写不敏感
     * @return hostname → IP 映射；无数据返回空 Map
     */
    Map<String, String> getAllHostnameToIp(String dcId);

    /**
     * 获取所有已缓存机房的完整地址快照，key 为大写 DC ID。
     */
    Map<String, KeeperContainerAddressSnapshot> getAllAddressSnapshots();

    /**
     * 不知道机房时解析 hostname 对应的 IP
     * <p>
     * 先查本机房，命中即返回；否则按机房名字典序取首个命中的远端机房，多个远端机房命中时打 WARN。
     *
     * @param hostname keeper container hostname
     * @return IP；未找到返回 null
     */
    String getIpByHostname(String hostname);

    /**
     * 按网络 IP 反查所属 keepercontainer，只认存活节点（覆盖同 hostname 下全部存活 pod 的 IP）
     * <p>
     * 各机房 pod IP 段可能重叠，返回全部候选，由调用方按上下文消歧。本机房结果在前，远端按机房名字典序；
     * 远端候选出自同一份快照。
     *
     * @param ip keepercontainer pod IP
     * @return 全部命中的 {@code (dc, hostname)}，dc 统一大写（与 meta 比较时需忽略大小写）；未命中返回空列表
     */
    List<Pair<String, String>> findHostnamesByIp(String ip);
}
