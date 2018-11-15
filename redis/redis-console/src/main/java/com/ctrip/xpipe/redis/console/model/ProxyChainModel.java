package com.ctrip.xpipe.redis.console.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.List;

public class ProxyChainModel {

    private String shardId;

    private HostPort redisMaster;

    private HostPort activeDcKeeper;

    private HostPort backupDcKeeper;

    private List<String> tunnelIds;
}
