package com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.diskless;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.RedisConfigCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 07, 2018
 */
public class DiskLessReplCheckAction extends RedisConfigCheckAction {

    private static final Logger logger = LoggerFactory.getLogger(DiskLessReplCheckAction.class);

    private static final String VERSION = "redis_version";

    public final static String REPL_DISKLESS_SYNC = "repl-diskless-sync";

    private String info;

    private volatile boolean isDiskLess, configReady;

    public DiskLessReplCheckAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance,
                                   ExecutorService executors, AlertManager alertManager) {
        super(scheduled, instance, executors, alertManager);
    }

    @Override
    protected void doTask() {
        if(checkDiskLessSetting()) {
            checkPassed();
            return;
        }
        getActionInstance().getRedisSession().infoServer(new Callbackable<String>() {
            @Override
            public void success(String message) {
                info = message;
            }

            @Override
            public void fail(Throwable throwable) {
                logger.error("[DiskLessReplCheckAction]{}", throwable);
            }
        });
        getActionInstance().getRedisSession().isDiskLessSync(new Callbackable<Boolean>() {
            @Override
            public void success(Boolean message) {
                configReady = true;
                isDiskLess = message;
            }

            @Override
            public void fail(Throwable throwable) {
                logger.error("[DiskLessReplCheckAction]{}", throwable);
            }
        });
    }


    private boolean checkDiskLessSetting() {
        if(info == null || !configReady) {
            return false;
        }
        if(!isDiskLess) {
            return true;
        }
        InfoResultExtractor extractor = new InfoResultExtractor(info);
        String version = extractor.extract(VERSION);
        String targetVersion = getActionInstance().getHealthCheckConfig().getMinDiskLessReplVersion();
        if(version != null && StringUtil.compareVersion(version, targetVersion) < 1) {
            String message = String.format("Redis should not set %s as YES", REPL_DISKLESS_SYNC);
            alertManager.alert(getActionInstance().getRedisInstanceInfo(), ALERT_TYPE.REDIS_REPL_DISKLESS_SYNC_ERROR, message);
            return false;
        }
        return true;
    }

    @VisibleForTesting
    protected DiskLessReplCheckAction setInfo(String info) {
        this.info = info;
        return this;
    }

    @VisibleForTesting
    protected DiskLessReplCheckAction setDiskLess(boolean diskLess) {
        isDiskLess = diskLess;
        return this;
    }

    @VisibleForTesting
    protected DiskLessReplCheckAction setConfigReady(boolean configReady) {
        this.configReady = configReady;
        return this;
    }
}
