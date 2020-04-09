package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class DefaultMetric implements MetricProxy{

    private ExecutorService executors;
    private List<MetricProxy> metricProxies = new LinkedList<>();
    private Logger logger = LoggerFactory.getLogger(getClass());

    public DefaultMetric(){

        executors = DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("Metric").createExecutorService();
        metricProxies.add(new HickwallMetric());
        metricProxies.add(new DashBoardMetric());
    }

    @Override
    public void writeBinMultiDataPoint(MetricData data) throws MetricProxyException {

        for(MetricProxy metricProxy : metricProxies){

            try{
                executors.execute(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        metricProxy.writeBinMultiDataPoint(data);
                    }
                });
            }catch (Exception e){
                logger.error("[writeBinMultiDataPoint]" + data, e);
            }
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
