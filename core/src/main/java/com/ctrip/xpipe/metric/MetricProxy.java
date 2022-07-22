package com.ctrip.xpipe.metric;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author marsqing
 *
 *         Dec 7, 2016 12:04:00 AM
 */
public interface MetricProxy extends Ordered {

	MetricProxy DEFAULT = ServicesUtil.getMetricProxy();

	void writeBinMultiDataPoint(MetricData data) throws MetricProxyException;

}
