package com.ctrip.xpipe.redis.console.health.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.health.RedisSessionManager;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 01, 2017
 */
@Component
@Lazy
public class DefaultRedisMasterCollector implements RedisMasterCollector{

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RedisSessionManager redisSessionManager;

    @Autowired
    private RedisService redisService;

    private ExecutorService executors;

    @PostConstruct
    public void postConstructDefaultRedisMasterCollector(){
        executors = DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("RedisMasterCorrector").createExecutorService();
    }

    @Override
    public void collect(Sample<InstanceRedisMasterResult> sample) {

        RedisMasterSamplePlan plan = (RedisMasterSamplePlan) sample.getSamplePlan();
        Map<HostPort, InstanceRedisMasterResult> hostPort2SampleResult = plan.getHostPort2SampleResult();
        if(hostPort2SampleResult.size() == 0 || hostPort2SampleResult.size() >= 2){
            logger.warn("[collect][size wrong]{}, {}", plan);
            correct(plan);
        }else {
            Map.Entry<HostPort, InstanceRedisMasterResult> next = hostPort2SampleResult.entrySet().iterator().next();
            if(next.getValue().roleIsSlave()){
                logger.info("[collect][role not right]{}, {}", plan, next);
                correct(plan);
            }
        }
    }

    private void correct(RedisMasterSamplePlan plan) {

        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                doCorrection(plan);
            }
        });
    }

    @VisibleForTesting
    protected void doCorrection(RedisMasterSamplePlan plan) {
        logger.info("[doCorrection]{}", plan);

        //check redis master again

        if(plan.getMasterHost() != null && isMaster(plan.getMasterHost(), plan.getMasterPort())){
            logger.info("[doCorrection][still master]{}", plan);
            return;
        }

        for (RedisMeta redisMeta : plan.getRedises()){

            if(redisMeta.getIp().equals(plan.getMasterHost()) && redisMeta.getPort().equals(plan.getMasterPort())){
                continue;
            }
            if (isMaster(redisMeta.getIp(), redisMeta.getPort())){
                try {
                    changeMasterRoleInDb(plan, redisMeta.getIp(), redisMeta.getPort());
                } catch (ResourceNotFoundException e) {
                    logger.error("doCorrection" + plan, e);
                }
            }
        }
    }

    private void changeMasterRoleInDb(RedisMasterSamplePlan plan, String newMasterIp, Integer newMasterPort) throws ResourceNotFoundException {
        logger.info("[changeMasterRoleInDb]{}, {}:{}", plan, newMasterIp, newMasterPort);

        List<RedisTbl> allByDcClusterShard = redisService.findAllByDcClusterShard(plan.getDcName(), plan.getClusterId(), plan.getShardId());
        boolean changed = false;
        for(RedisTbl redisTbl : allByDcClusterShard){
            if(newMasterIp.equalsIgnoreCase(redisTbl.getRedisIp())
                    && newMasterPort.equals(redisTbl.getRedisPort())){
                if(!redisTbl.isMaster()){
                    redisTbl.setMaster(true);
                    changed = true;
                }
            }else if(redisTbl.isMaster()){
                redisTbl.setMaster(false);
                changed = true;
            }
        }

        logger.info("[changeMasterRoleInDb]{}", changed);
        if(changed){
            redisService.updateBatchMaster(allByDcClusterShard);
        }
    }

    @VisibleForTesting
    protected boolean isMaster(String host, int port) {
        try {
            RedisSession redisSession = redisSessionManager.findOrCreateSession(host, port);
            String role = redisSession.roleSync();
            if(Server.SERVER_ROLE.MASTER.sameRole(role)){
                return true;
            }
        } catch (Exception e) {
            logger.error(String.format("%s:%d", host, port), e);
        }
        return  false;
    }

    @PreDestroy
    public void preDestroyDefaultRedisMasterCollector(){
        executors.shutdown();
    }

}
