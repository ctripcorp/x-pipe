package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * hostname → IP 单向解析，数据来源是本机房 ZK 注册路径下由 keepercontainer 自注册的临时节点。
 * <p>
 * 外部系统（metaserver/checker）从 meta 得到 hostname，自己 watch ZK 得到 IP，
 * 调用 console API 时同时携带 hostname + IP，无需 console 反向查询。
 * 因此只提供 hostname→IP 单向映射，不提供 IP→hostname 反向映射。
 * <p>
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
}
