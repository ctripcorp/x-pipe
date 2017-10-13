package com.ctrip.xpipe.metric;

import com.ctrip.xpipe.api.lifecycle.Ordered;

import java.util.List;

/**
 * @author marsqing
 *
 *         Dec 7, 2016 12:04:00 AM
 */
public interface MetricProxy extends Ordered {

	void writeBinMultiDataPoint(List<MetricData> datas) throws MetricProxyException;

}
