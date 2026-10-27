package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import org.springframework.stereotype.Component;

/**
 * 只采 TFS Keeper。范围与 {@code KeeperDelayActionController} 一致：实例本身已由
 * {@code KeeperCheckSelector} 限定为「Checker 位于 ActiveDc 且与 ActiveDc 同 Region」的 ONE_WAY Keeper。
 */
@Component
public class KeeperStateActionController implements HealthCheckActionController<KeeperHealthCheckInstance> {

    @Override
    public boolean shouldCheck(KeeperHealthCheckInstance instance) {
        return instance.isTfs();
    }
}
