package com.ctrip.xpipe.redis.console.health.action;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
@Component
@Lazy
public class OuterClientServiceProcessor implements HealthEventProcessor{

    private OuterClientService outerClientService = OuterClientService.DEFAULT;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private AllMonitorCollector allMonitorCollector;

    @Override
    public void onEvent(AbstractInstanceEvent instanceEvent) throws Exception {

        if(!instanceInBackupDc(instanceEvent.getHostPort())){
            logger.info("[onEvent][instance not in backupDc]{}", instanceEvent.getHostPort());
            return;
        }

        if(instanceEvent instanceof InstanceUp){
            outerClientService.markInstanceUp(instanceEvent.getHostPort());
        }else if(instanceEvent instanceof InstanceDown){

            if(masterUp(instanceEvent.getHostPort())){
                quorumMarkInstanceDown(instanceEvent.getHostPort());
            }else{
                logger.info("[onEvent][master down, do not call client service]{}", instanceEvent);
            }
        }else{
            throw new IllegalStateException("unknown event:" + instanceEvent);
        }
    }

    private void quorumMarkInstanceDown(HostPort hostPort) throws Exception {


        outerClientService.markInstanceDown(hostPort);
    }

    private boolean instanceInBackupDc(HostPort hostPort) {
        return metaCache.inBackupDc(hostPort);
    }

    private boolean masterUp(HostPort hostPort) {

        //master up
        HostPort redisMaster = metaCache.findMasterInSameShard(hostPort);
        boolean masterUp = allMonitorCollector.getState(redisMaster) == HEALTH_STATE.UP;
        if(!masterUp){
            logger.info("[masterUp][master down instance:{}, master:{}]", hostPort, redisMaster);
        }
        return masterUp;
    }
}