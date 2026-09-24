package com.ctrip.xpipe.redis.console.service;

import java.util.Map;

/**
 * 跨机房 hostname → IP 解析服务
 * <p>
 * 提供两种解析方式：
 * 1. 单个 hostname 解析：自动判断目标机房并转发请求
 * 2. 批量查询整个机房的映射：一次性获取指定机房所有 hostname→IP 映射
 * <p>
 * 本机房：直接从本地缓存读取（CurrentDcKeeperContainerAddressService）
 * 其他机房：转发到目标机房的 console API
 */
public interface CrossDcKeeperContainerAddressService {

    /**
     * 解析单个 hostname 对应的 IP
     * <p>
     * 自动判断 hostname 所属机房：
     * - 本机房：直接从本地缓存读取
     * - 其他机房：调用目标机房 console 的 API
     *
     * @param dcId     机房 ID
     * @param hostname keeper container hostname
     * @return IP 地址；未找到或调用失败返回 null
     */
    String getIpByHostname(String dcId, String hostname);

    /**
     * 获取指定机房所有 keeper container 的 hostname → IP 映射
     * <p>
     * 适用场景：
     * - 需要批量解析多个 hostname 时，减少跨机房调用次数
     * - 需要预加载映射表提高性能
     *
     * @param dcId 机房 ID
     * @return hostname → IP 映射表；调用失败返回空 Map
     */
    Map<String, String> getAllHostnameToIp(String dcId);

    /**
     * 解析 hostname 对应的 IP（自动查询所有机房）
     * <p>
     * 查询逻辑：
     * 1. 先查本机房缓存
     * 2. 如果本机房没有，并发查询所有其他机房
     * 3. 返回第一个找到的结果
     *
     * @param hostname keeper container hostname
     * @return IP 地址；未找到返回 null
     */
    String getIpByHostname(String hostname);
}
