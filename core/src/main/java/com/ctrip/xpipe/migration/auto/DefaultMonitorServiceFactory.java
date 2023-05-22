package com.ctrip.xpipe.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.MonitorServiceFactory;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class DefaultMonitorServiceFactory implements MonitorServiceFactory {

    @Override
    public MonitorService build(String host) {
        return new DefaultMonitorService(host);
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

}
