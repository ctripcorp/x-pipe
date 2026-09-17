package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * hostname 与 IP 的双向解析，数据来源是本机房 ZK 注册路径下由 keepercontainer 自注册的临时节点。
 * <p>
 * 所有需要"用 hostname 反查 IP"或"用 IP 反查 hostname"的入口都只依赖本接口，不直接访问 ZK。
 * console、metaserver、checker 各自装配一份，watch 的都是本机房那套 ZK；跨机房解析不走这里，
 * 而是转发给目标机房的 console。
 * <p>
 * 只解析 IP、不返回端口：容器管理面端口与 keeper 实例端口是两套语义（前者固定 8080，
 * 后者从 6380 起递增），端口该取哪个由调用方按自身场景决定，本服务不做假设。
 */
public interface KeeperContainerAddressService extends Lifecycle {

    /**
     * @param hostname pod hostname
     * @return 该 hostname 当前注册的 IP；容器未注册或已下线时返回 null
     */
    String getIpByHostname(String hostname);

    /**
     * @param ip 容器 IP
     * @return 当前持有该 IP 的 pod hostname；查不到返回 null，
     *         意味着这是物理机 IP，或是 pod 重建前的过期 IP
     */
    String getHostnameByIp(String ip);
}
