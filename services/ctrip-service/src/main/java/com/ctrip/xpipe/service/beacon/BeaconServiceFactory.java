package com.ctrip.xpipe.service.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.MonitorServiceFactory;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class BeaconServiceFactory implements MonitorServiceFactory {

    @Override
    public MonitorService build(String host) {
        return new BeaconService(host);
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }

}
