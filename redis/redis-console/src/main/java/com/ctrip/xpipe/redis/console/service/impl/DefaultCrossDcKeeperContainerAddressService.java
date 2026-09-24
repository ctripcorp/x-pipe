package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;

/**
 * 跨机房 hostname → IP 解析服务实现
 * <p>
 * 本机房：直接使用 CurrentDcKeeperContainerAddressService
 * 其他机房：通过 ConsoleServiceManager 调用目标机房的 console API
 */
@Service
public class DefaultCrossDcKeeperContainerAddressService implements CrossDcKeeperContainerAddressService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCrossDcKeeperContainerAddressService.class);

    private final String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();

    @Autowired
    private CurrentDcKeeperContainerAddressService currentDcAddressService;

    @Autowired
    private ConsoleServiceManager consoleServiceManager;


    @Override
    public String getIpByHostname(String dcId, String hostname) {
        if (hostname == null || hostname.isEmpty()) {
            return null;
        }

        if (dcId == null || dcId.isEmpty()) {
            logger.warn("[getIpByHostname] dcId is empty, hostname: {}", hostname);
            return null;
        }

        // 本机房：直接使用本地缓存
        if (currentDc.equalsIgnoreCase(dcId)) {
            return currentDcAddressService.getIpByHostname(hostname);
        }

        // 跨机房：通过 ConsoleServiceManager 调用目标机房的 console
        try {
            return consoleServiceManager.resolveKeeperContainerIp(dcId, hostname);
        } catch (Exception e) {
            logger.error("[getIpByHostname] failed to resolve hostname from dc={}, hostname={}", dcId, hostname, e);
            return null;
        }
    }

    @Override
    public Map<String, String> getAllHostnameToIp(String dcId) {
        if (dcId == null || dcId.isEmpty()) {
            logger.warn("[getAllHostnameToIpMap] dcId is empty");
            return Collections.emptyMap();
        }

        // 本机房：直接从本地缓存获取
        if (currentDc.equalsIgnoreCase(dcId)) {
            return currentDcAddressService.getAllHostnameToIp();
        }

        // 跨机房：通过 ConsoleServiceManager 调用目标机房的 console
        try {
            return consoleServiceManager.getAllKeeperContainerHostnameToIpMap(dcId);
        } catch (Exception e) {
            logger.error("[getAllHostnameToIpMap] failed to get hostname map from dc={}", dcId, e);
            return Collections.emptyMap();
        }
    }

    @Override
    public String getIpByHostname(String hostname) {
        if (hostname == null || hostname.isEmpty()) {
            return null;
        }

        // 先查本机房
        String ip = currentDcAddressService.getIpByHostname(hostname);
        if (ip != null) {
            logger.debug("[getIpByHostname] found in local dc, hostname={}, ip={}", hostname, ip);
            return ip;
        }

        // 本机房没有，并发查询所有其他机房
        logger.info("[getIpByHostname] not found in local dc, broadcasting to all dcs, hostname={}", hostname);
        return consoleServiceManager.broadcastResolveKeeperContainerIp(hostname);
    }
}
