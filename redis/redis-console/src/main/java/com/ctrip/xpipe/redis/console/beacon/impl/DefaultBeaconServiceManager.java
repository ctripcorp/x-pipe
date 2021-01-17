package com.ctrip.xpipe.redis.console.beacon.impl;

import com.ctrip.xpipe.redis.console.beacon.BeaconService;
import com.ctrip.xpipe.redis.console.beacon.BeaconServiceManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/1/15
 */
@Service
public class DefaultBeaconServiceManager implements BeaconServiceManager {

    private ConsoleConfig config;

    private Map<String, BeaconService> beaconServiceMap;

    @Autowired
    public DefaultBeaconServiceManager(ConsoleConfig config) {
        this.config = config;
        this.beaconServiceMap = Maps.newHashMap();
    }

    @Override
    public BeaconService getOrCreate(long orgId) {
        String host = getAddrByOrgId(orgId);
        if (StringUtil.isEmpty(host)) return null;

        return getOrCreate(host);
    }

    @Override
    public Map<Long, BeaconService> getAllServices() {
        Map<Long, String> hostsByOrg = config.getBeaconHosts();
        String defaultHost = config.getDefaultBeaconHost();
        Map<Long, BeaconService> services = new HashMap<>(hostsByOrg.size() + 1);

        if (!StringUtil.isEmpty(defaultHost)) services.put(XPipeConsoleConstant.DEFAULT_ORG_ID, getOrCreate(defaultHost));
        hostsByOrg.forEach((orgId, host) -> services.put(orgId, getOrCreate(host)));

        return services;
    }

    private BeaconService getOrCreate(String host) {
        return MapUtils.getOrCreate(beaconServiceMap, host, () -> new DefaultBeaconService(host));
    }

    private String getAddrByOrgId(long orgId) {
        String host = config.getBeaconHosts().get(orgId);
        if (!StringUtil.isEmpty(host)) return host;

        host = config.getDefaultBeaconHost();
        if (!StringUtil.isEmpty(host)) return host;

        return null;
    }

}
