package com.ctrip.xpipe.api.migration.auto;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public interface MonitorServiceFactory extends Ordered {

    MonitorServiceFactory DEFAULT = ServicesUtil.getMonitorServiceFactory();

    MonitorService build(String name, String host, int weight);

}
