package com.ctrip.xpipe.redis.console.health.migration.version;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.*;
import com.ctrip.xpipe.redis.console.health.migration.AbstractPreMigrationHealthCheck;
import com.ctrip.xpipe.redis.console.health.migration.Callbackable;
import com.ctrip.xpipe.redis.console.health.migration.version.VersionCallback;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Sep 21, 2017
 */

@Component
//@ConditionalOnProperty(name = {HealthChecker.ENABLED}, matchIfMissing = true)
public class VersionCheck extends AbstractPreMigrationHealthCheck {

    @Autowired
    VersionCallback versionCallback;

    @Override
    protected void doCheck() {
        logger.info("[doCheck] start");
        List<RedisMeta> backupRedises = getBackupRedises();
        logger.info("[doCheck]Redises to check: {}", backupRedises);
        for(RedisMeta redisMeta : backupRedises) {
            RedisSession redisSession = redisSessionManager
                    .findOrCreateSession(redisMeta.getIp(), redisMeta.getPort());
            try {
                redisSession.serverInfo(versionCallback);
            } catch (IllegalStateException e) {
                alertManager.alert();
            }
        }
    }

    @Override
    public void fail(Throwable throwable) {

    }

    private List<RedisMeta> getBackupRedises() {
        List<RedisMeta> allRedises = getAllRedises();
        return allRedises.stream()
                .filter(redisMeta -> {return metaCache
                        .inBackupDc(new HostPort(redisMeta.getIp(), redisMeta.getPort()));})
                .collect(Collectors.toList());
    }
}
