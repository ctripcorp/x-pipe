package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public class SentinelSamplePlan extends BaseSamplePlan<InstanceSentinelResult>{

    private ConsoleConfig consoleConfig;

    public SentinelSamplePlan(String clusterId, String shardId, ConsoleConfig consoleConfig) {
        super(clusterId, shardId);
        this.consoleConfig = consoleConfig;
    }

    @Override
    public void addRedis(String dcId, RedisMeta redisMeta, InstanceSentinelResult initSampleResult) {

        if(!isOnlyActiveDcAvailableCheck(dcId, redisMeta) && isActiveDcRedis(dcId, redisMeta)){
            return;
        }
        super.addRedis(dcId, redisMeta, initSampleResult);
    }

    private boolean isActiveDcRedis(String dcId, RedisMeta redisMeta) {
        return redisMeta.parent().getActiveDc().equalsIgnoreCase(dcId);
    }

    @VisibleForTesting
    protected boolean isOnlyActiveDcAvailableCheck(String dcId, RedisMeta redisMeta) {
        String backupDcs = redisMeta.parent().getBackupDcs();
        String[] backupDcArr = StringUtil.splitRemoveEmpty("\\s*,\\s*", backupDcs);
        Set<String> ignoredHealthCheckDc = consoleConfig.getIgnoredHealthCheckDc();
        int count = 0;
        for(String backupDc : backupDcArr) {
            if(ignoredHealthCheckDc.contains(backupDc)) {
                count ++;
            }
        }
        return count == backupDcArr.length;
    }

}
