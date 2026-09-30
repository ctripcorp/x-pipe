package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 跨机房 keepercontainer 地址查询门面（keeper-dynamic-ip spec §8.5），无状态、只读内存：
 * <ul>
 *     <li>本机房：{@link CurrentDcKeeperContainerAddressService}</li>
 *     <li>远端机房：{@link RemoteDcKeeperContainerAddressCache} 的快照</li>
 * </ul>
 * 不知道机房的两个查询由缓存在同一份快照内完成，门面只拼接本机房结果，不遍历机房。
 */
@Service
public class DefaultCrossDcKeeperContainerAddressService implements CrossDcKeeperContainerAddressService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultCrossDcKeeperContainerAddressService.class);

    private final CurrentDcKeeperContainerAddressService currentDcAddressService;

    private final RemoteDcKeeperContainerAddressCache remoteDcAddressCache;

    private final String currentDc;

    @Autowired
    public DefaultCrossDcKeeperContainerAddressService(CurrentDcKeeperContainerAddressService currentDcAddressService,
                                                       RemoteDcKeeperContainerAddressCache remoteDcAddressCache) {
        this(currentDcAddressService, remoteDcAddressCache, FoundationService.DEFAULT.getDataCenter());
    }

    @VisibleForTesting
    DefaultCrossDcKeeperContainerAddressService(CurrentDcKeeperContainerAddressService currentDcAddressService,
                                                RemoteDcKeeperContainerAddressCache remoteDcAddressCache,
                                                String currentDc) {
        this.currentDcAddressService = currentDcAddressService;
        this.remoteDcAddressCache = remoteDcAddressCache;
        this.currentDc = currentDc.toUpperCase();
    }

    @Override
    public String getIpByHostname(String dcId, String hostname) {
        if (StringUtil.isEmpty(hostname)) {
            return null;
        }
        if (StringUtil.isEmpty(dcId)) {
            logger.warn("[getIpByHostname] dcId is empty, hostname: {}", hostname);
            return null;
        }
        if (isCurrentDc(dcId)) {
            return currentDcAddressService.getIpByHostname(hostname);
        }
        return remoteDcAddressCache.getIpByHostname(dcId, hostname);
    }

    @Override
    public Map<String, String> getAllHostnameToIp(String dcId) {
        if (StringUtil.isEmpty(dcId)) {
            logger.warn("[getAllHostnameToIp] dcId is empty");
            return Collections.emptyMap();
        }
        if (isCurrentDc(dcId)) {
            return currentDcAddressService.getAllHostnameToIp();
        }
        return remoteDcAddressCache.getAllHostnameToIp(dcId);
    }

    @Override
    public String getIpByHostname(String hostname) {
        if (StringUtil.isEmpty(hostname)) {
            return null;
        }

        String ip = currentDcAddressService.getIpByHostname(hostname);
        if (null != ip) {
            return ip;
        }

        List<Pair<String, String>> remote = remoteDcAddressCache.findIpsByHostname(hostname);
        if (remote.isEmpty()) {
            return null;
        }
        if (remote.size() > 1) {
            // hostname 全局唯一是前提（spec §8.8），多机房命中说明前提被打破，按字典序取首
            logger.warn("[getIpByHostname] hostname {} found in multiple dcs {}, use {}", hostname, remote, remote.get(0));
        }
        return remote.get(0).getValue();
    }

    @Override
    public List<Pair<String, String>> findHostnamesByIp(String ip) {
        if (StringUtil.isEmpty(ip)) {
            return new ArrayList<>();
        }

        List<Pair<String, String>> result = new ArrayList<>();
        String localHostname = currentDcAddressService.getHostnameByIp(ip);
        if (null != localHostname) {
            result.add(Pair.of(currentDc, localHostname));
        }
        result.addAll(remoteDcAddressCache.findHostnamesByIp(ip));
        return result;
    }

    private boolean isCurrentDc(String dcId) {
        return currentDc.equalsIgnoreCase(dcId);
    }
}
