package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.Map;

/**
 * 本机房 keepercontainer 的 hostname ↔ IP 解析，数据来源是本机房 ZK 注册路径下由 keepercontainer 自注册的临时节点。
 * <p>
 * 正反两个方向语义不对称，各自贴合用途：
 * <ul>
 *     <li>hostname → IP：取同 hostname 下最早注册、仍存活的 pod；hostname 从 ZK 消失后保留 last-known IP 一段时间
 *     （tombstone，默认 1h）再移除。用途是拿到唯一确定的调用目标，包括对已消失的 pod 收尾。
 *     拿到过期 IP 发直连命令时由容器端 {@link HostNameVerification} 校验挡住（421）。</li>
 *     <li>IP → hostname：只认存活节点，tombstone 不参与；覆盖同 hostname 下全部存活 pod 的 IP。
 *     用途是无盲区的归属判定。</li>
 * </ul>
 * <p>
 * console、metaserver、checker 各自装配一份，watch 的都是本机房那套 ZK；跨机房解析不走这里，
 * 而是由 console 周期拉取各远端机房的 {@link #getAddressSnapshot()} 后在本地查询（keeper-dynamic-ip spec §8.5）。
 * <p>
 * 只解析 IP、不返回端口：容器管理面端口与 keeper 实例端口是两套语义（前者固定 8080，
 * 后者从 6380 起递增），端口该取哪个由调用方按自身场景决定，本服务不做假设。
 */
public interface CurrentDcKeeperContainerAddressService extends Lifecycle {

    /**
     * 解析单个 hostname 对应的 IP
     *
     * @param hostname pod hostname
     * @return 该 hostname 首个存活 pod 的 IP；hostname 已从 ZK 消失但仍在 tombstone 保留期内时返回 last-known IP；
     * 从未注册或保留期已过返回 null
     */
    String getIpByHostname(String hostname);

    /**
     * 获取本机房所有 keeper container 的 hostname → IP 映射
     * <p>
     * 用于批量查询场景，避免多次调用 getIpByHostname。与逐个调用 {@link #getIpByHostname} 结果一致，
     * 即同样包含保留期内的 tombstone。
     *
     * @return hostname → IP 映射表的快照
     */
    Map<String, String> getAllHostnameToIp();

    /**
     * 按 IP 反查 hostname，只认存活节点
     *
     * @param ip pod IP
     * @return 当前有存活 pod 注册该 IP 的 hostname；多个 hostname 同时命中时按 hostname 字典序取首；
     * 无存活 pod 持有该 IP 时返回 null（不回落到 tombstone）
     */
    String getHostnameByIp(String ip);

    /**
     * 导出当前地址映射的完整快照，供远端机房 console 批量拉取（spec §8.3）
     * <p>
     * 三部分出自同一份内部快照：按快照里的规则查询（正向先存活后 tombstone、反向只认存活），
     * 与在本机房逐个调 {@link #getIpByHostname} / {@link #getHostnameByIp} 结果一致。
     * tombstone 只含未到期项，剩余保留期按毫秒向下取整，不足 1ms 的不导出。
     *
     * @return 新建的快照，与内部状态不共享 map
     */
    KeeperContainerAddressSnapshot getAddressSnapshot();
}
