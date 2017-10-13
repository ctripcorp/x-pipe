package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class DefaultMetric implements MetricProxy{

    private ExecutorService executors;
    private List<MetricProxy> metricProxies = new LinkedList<>();

    public DefaultMetric(){
        this.executors = Executors.newCachedThreadPool(XpipeThreadFactory.create("Metric"));
        metricProxies.add(new HickwallMetric());
        metricProxies.add(new DashBoardMetric());
    }

    @Override
    public void writeBinMultiDataPoint(List<MetricData> datas) throws MetricProxyException {

        for(MetricProxy metricProxy : metricProxies){

            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {
                    metricProxy.writeBinMultiDataPoint(datas);
                }
            });
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
