package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoReplicationComplementCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.RedisInfo;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveInfo;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.dcchange.ExecutionLog;
import com.ctrip.xpipe.redis.meta.server.dcchange.OffsetWaiter;
import com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.StringUtil;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 13, 2017
 */
@Component
public class DefaultOffsetwaiter implements OffsetWaiter {

    @Resource(name = MetaServerContextConfig.CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    @Resource(name = AbstractSpringConfigContext.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private MetaServerConfig metaServerConfig;

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public boolean tryWaitfor(HostPort hostPort, MasterInfo masterInfo, ExecutionLog executionLog) {

        if(hostPort == null){
            executionLog.info("target instance null");
            return false;
        }

        if (masterInfo == null) {
            executionLog.info("master info null, no wait");
            return false;
        }

        String masterReplId = masterInfo.getReplId();
        Long masterOffset = masterInfo.getMasterReplOffset();
        if (masterOffset == null || masterOffset <= 0) {
            executionLog.info("master offset wrong, no wait" + masterOffset);
            return false;
        }

        executionLog.info(String.format("wait for %s %s", hostPort, masterInfo));

        try {
            return doWait(masterReplId, masterOffset, hostPort, executionLog);
        } catch (Exception e) {
            logger.error("[tryWaitfor]" + hostPort + "," + masterInfo, e);
            executionLog.error(e.getMessage());
        }
        return  false;
    }

    private boolean doWait(String masterReplId, Long masterOffset, HostPort hostPort, ExecutionLog executionLog) {

        int waitMilli = metaServerConfig.getWaitforOffsetMilli();
        long endTime = System.currentTimeMillis() + waitMilli;

        String slaveReplId = null;
        Long slaveOffset = null;

        executionLog.info(String.format("wait timeout config:%d ms", waitMilli));


        while (true) {

            InfoReplicationComplementCommand command = new InfoReplicationComplementCommand(
                    keyedObjectPool.getKeyPool(new DefaultEndPoint(hostPort.getHost(), hostPort.getPort())),
                    scheduled
            );

            try {

                RedisInfo redisInfo = command.execute().get(waitMilli, TimeUnit.MILLISECONDS);
                if ((redisInfo instanceof MasterInfo) || !(redisInfo instanceof SlaveInfo)) {
                    executionLog.info("target role:" + (redisInfo == null ? "null" : redisInfo.getClass().getSimpleName()));
                    break;
                }

                SlaveInfo slaveInfo = (SlaveInfo) redisInfo;
                slaveReplId = slaveInfo.getMasterReplId();
                slaveOffset = slaveInfo.getSlaveReplOffset();

                if (!StringUtil.isEmpty(slaveReplId) && !StringUtil.isEmpty(masterReplId)) {
                    if (!slaveReplId.equalsIgnoreCase(masterReplId)) {
                        executionLog.info(String.format("master replid not equal with slave replid, break. %s %s", masterReplId, slaveReplId));
                        break;
                    }
                }
                if (slaveOffset >= masterOffset) {
                    executionLog.info(String.format("wait succeed:%d >= %d", slaveOffset, masterOffset));
                    return true;
                }
            } catch (Exception e) {
                executionLog.error(e.getMessage());
                logger.error("[waitfor]" + hostPort, e);
            }
            long current = System.currentTimeMillis();
            if (current >= endTime) {
                executionLog.error("wait time out, exit");
                break;
            }
            sleep(1);
        }

        if(slaveOffset != null){
            executionLog.info(String.format("master offset:%s, slave offset:%d, sub:%d", masterOffset, slaveOffset, masterOffset - slaveOffset));
        }
        return false;
    }

    private void sleep(int time) {
        try {
            TimeUnit.MILLISECONDS.sleep(time);
        } catch (InterruptedException e) {
            logger.error("[sleep]", e);
        }
    }


    public void setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
    }

    public void setMetaServerConfig(MetaServerConfig metaServerConfig) {
        this.metaServerConfig = metaServerConfig;
    }

    public void setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }
}
