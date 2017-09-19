package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
public abstract class AbstractRedisConfMonitor<T extends BaseInstanceResult> extends BaseSampleMonitor<T>{

    private long lastPlanTime = 0;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;


    @Override
    public Collection<BaseSamplePlan<T>> generatePlan(List<DcMeta> dcMetas) {

        if(!shouldStart()){
            return null;
        }

        if(clusterServer != null && !clusterServer.amILeader()){
            log.debug("[generatePlan][not leader quit]");
            return null;
        }


        long current = System.currentTimeMillis();
        if( current - lastPlanTime < consoleConfig.getRedisConfCheckIntervalMilli()){
            log.debug("[generatePlan][too quick {}, quit]", current - lastPlanTime);
            return null;
        }

        lastPlanTime = current;
        return super.generatePlan(dcMetas);
    }

    protected boolean shouldStart(){
        return true;
    }

    @Override
    public void startSample(BaseSamplePlan<T> plan) throws SampleException{

        if(plan == null){
            return;
        }
        doStartSample(plan);
    }

    protected abstract void doStartSample(BaseSamplePlan<T> plan);

}
