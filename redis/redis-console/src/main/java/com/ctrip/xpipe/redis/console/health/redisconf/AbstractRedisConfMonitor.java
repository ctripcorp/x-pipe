package com.ctrip.xpipe.redis.console.health.redisconf;

import com.ctrip.xpipe.cluster.ClusterServer;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.BaseSampleMonitor;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
public abstract class AbstractRedisConfMonitor extends BaseSampleMonitor<InstanceRedisConfResult>{

    private long lastPlanTime = 0;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired(required = false)
    private ClusterServer clusterServer;

    @Autowired
    private List<RedisConfCollector> collectors;

    @Override
    protected void notifyCollectors(Sample<InstanceRedisConfResult> sample) {

        collectors.forEach((collector) -> collector.collect(sample));
    }

    @Override
    public Collection<BaseSamplePlan<InstanceRedisConfResult>> generatePlan(List<DcMeta> dcMetas) {

        if(clusterServer != null && !clusterServer.amILeader()){
            log.debug("[generatePlan][not leader quit]");
            return null;
        }

        long current = System.currentTimeMillis();
        if( current - lastPlanTime < consoleConfig.getRedisConfCheckIntervalMilli()){
            log.debug("[generatePlan][too quick {}, not leader quit]", current - lastPlanTime);
            return null;
        }

        lastPlanTime = current;
        return super.generatePlan(dcMetas);
    }

    @Override
    public void startSample(BaseSamplePlan<InstanceRedisConfResult> plan) throws Exception {

        if(plan == null){
            return;
        }
        doStartSample(plan);
    }

    protected abstract void doStartSample(BaseSamplePlan<InstanceRedisConfResult> plan);

}
