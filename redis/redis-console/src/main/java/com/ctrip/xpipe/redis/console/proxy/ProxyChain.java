package com.ctrip.xpipe.redis.console.proxy;

import java.util.List;

public interface ProxyChain {

    String getBackupDc();

    String getCluster();

    String getShard();

    List<TunnelInfo> getTunnels();

}
