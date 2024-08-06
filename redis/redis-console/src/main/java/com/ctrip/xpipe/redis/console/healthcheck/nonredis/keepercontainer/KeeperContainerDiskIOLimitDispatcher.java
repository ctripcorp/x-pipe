package com.ctrip.xpipe.redis.console.healthcheck.nonredis.keepercontainer;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.KeeperContainerService;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractSiteLeaderIntervalAction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_CONTAINER_IO_RATE;

/**
 * @author lishanglin
 * date 2024/7/30
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class KeeperContainerDiskIOLimitDispatcher extends AbstractSiteLeaderIntervalAction {

    @Autowired
    private KeeperContainerService keeperContainerService;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private ConfigService configService;

    @Autowired
    private ConsoleConfig config;

    private AtomicBoolean running = new AtomicBoolean(false);

    private static final String ALERT_EVENT = "KeeperIOLimit";

    @Override
    protected void doAction() {
        if (running.compareAndSet(false, true)) {
            try {
                run();
            } finally {
                running.set(false);
            }
        }
    }

    protected void run() {
        List<KeeperContainerMeta> keeperContainerMetas = fetchCurrentDcKeeperContainers();
        Map<String, Integer> diskIOLimits = getDiskIOLimits();

        for (KeeperContainerMeta keepercontainer: keeperContainerMetas) {
            Integer limit = diskIOLimits.get(keepercontainer.getDiskType().toUpperCase());
            if (null == limit) {
                logger.info("[doAction][unknown limit] {}", keepercontainer);
                EventMonitor.DEFAULT.logEvent(ALERT_EVENT, "unknownLimit");
            } else {
                try {
                    keeperContainerService.setKeeperContainerDiskIOLimit(keepercontainer.getIp(), keepercontainer.getPort(), megaByte2Byte(limit));
                    EventMonitor.DEFAULT.logEvent(ALERT_EVENT, "success");
                } catch (Throwable th) {
                    logger.warn("[doAction][fail] {}", keepercontainer, th);
                    EventMonitor.DEFAULT.logEvent(ALERT_EVENT, "accessFail");
                }
            }
        }
    }

    private int megaByte2Byte(int mb) {
        return Math.max(0, mb * 1024 * 1024);
    }

    private List<KeeperContainerMeta> fetchCurrentDcKeeperContainers() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return Collections.emptyList();

        DcMeta dcMeta = xpipeMeta.getDcs().get(FoundationService.DEFAULT.getDataCenter());
        if (null == dcMeta) return Collections.emptyList();

        return dcMeta.getKeeperContainers();
    }

    private Map<String, Integer> getDiskIOLimits() {
        List<ConfigModel> configModels = configService.getConfigs(KEY_KEEPER_CONTAINER_IO_RATE);
        Map<String, Integer> diskIOLimits = new HashMap<>();
        for (ConfigModel configModel: configModels) {
            try {
                diskIOLimits.put(configModel.getSubKey().toUpperCase(), Integer.parseInt(configModel.getVal()));
            } catch (Throwable th) {
                logger.info("[getDiskIOLimits][fail] {}", configModel, th);
            }
        }
        return diskIOLimits;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }

    @Override
    protected boolean shouldDoAction() {
        return config.autoSetKeeperSyncLimit();
    }

}
