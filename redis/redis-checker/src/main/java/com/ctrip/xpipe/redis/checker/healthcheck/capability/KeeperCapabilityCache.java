package com.ctrip.xpipe.redis.checker.healthcheck.capability;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractConfigCommand.REDIS_CONFIG_TYPE;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Keeper capability cache populated by the independent refresh manager; reads are memory-only. */
@Component
public class KeeperCapabilityCache {

    private static final Logger logger = LoggerFactory.getLogger(KeeperCapabilityCache.class);
    private static final String PREPARE_WATCH = REDIS_CONFIG_TYPE.PREPARE_WATCH.getConfigName();
    private static final String PUBSUB_PARSE = REDIS_CONFIG_TYPE.PUBSUB_PARSE.getConfigName();
    private static final String CAPABILITY_EVENT = "Keeper.Capability";

    public enum Capability {
        SUPPORTED,
        UNSUPPORTED,
        UNKNOWN
    }

    private final Object stateLock = new Object();
    private final ConcurrentMap<HostPort, CacheEntry> values = new ConcurrentHashMap<>();
    private final Map<HostPort, RefreshAttempt> inflight = new HashMap<>();
    private final Map<HostPort, Long> addressGenerations = new HashMap<>();
    private final Map<String, Long> dcGenerations = new HashMap<>();
    private long lifecycleGeneration;

    public Capability get(HostPort address) {
        if (address == null) {
            return Capability.UNKNOWN;
        }
        CacheEntry entry = values.get(address);
        return entry == null ? Capability.UNKNOWN : entry.capability;
    }

    public Capability getIfPresent(HostPort address) {
        return get(address);
    }

    /** Starts one asynchronous refresh per address; failures preserve the last successful value. */
    public void refresh(KeeperHealthCheckInstance instance) {
        try {
            KeeperInstanceInfo info = instance == null ? null : instance.getCheckInfo();
            RedisSession session = instance == null ? null : instance.getRedisSession();
            if (info == null || info.getHostPort() == null || info.getDcId() == null || session == null) {
                logger.warn("[refresh][skip] invalid keeper instance {}", instance);
                return;
            }

            HostPort source = info.getHostPort();
            HostPort address = new HostPort(source.getHost(), source.getPort());
            RefreshAttempt attempt;
            synchronized (stateLock) {
                if (inflight.containsKey(address)) {
                    return;
                }
                attempt = new RefreshAttempt(address, info.getDcId(), session,
                        lifecycleGeneration,
                        addressGenerations.getOrDefault(address, 0L),
                        dcGenerations.getOrDefault(info.getDcId(), 0L));
                inflight.put(address, attempt);
            }
            attempt.start();
        } catch (Throwable throwable) {
            logger.warn("[refresh] failed to start Keeper capability refresh", throwable);
        }
    }

    /** Prevents old callbacks from writing into a replacement at this address. */
    public void invalidate(HostPort address) {
        if (address == null) {
            return;
        }
        synchronized (stateLock) {
            addressGenerations.put(address, addressGenerations.getOrDefault(address, 0L) + 1);
            values.remove(address);
            inflight.remove(address);
        }
    }

    /** Invalidates a DC generation and acts as its manager-stop gate. */
    public void invalidateDc(String dcId) {
        if (dcId == null) {
            return;
        }
        synchronized (stateLock) {
            dcGenerations.put(dcId, dcGenerations.getOrDefault(dcId, 0L) + 1);
            for (Map.Entry<HostPort, CacheEntry> entry : values.entrySet()) {
                if (dcId.equals(entry.getValue().dcId)) {
                    values.remove(entry.getKey(), entry.getValue());
                }
            }
            inflight.entrySet().removeIf(entry -> dcId.equals(entry.getValue().dcId));
        }
    }

    /** Advances the lifecycle gate and drops all cached or in-flight state. */
    public void invalidateAll() {
        synchronized (stateLock) {
            lifecycleGeneration++;
            values.clear();
            inflight.clear();
        }
    }

    private boolean isCurrent(RefreshAttempt attempt) {
        return attempt.lifecycleGeneration == lifecycleGeneration
                && attempt.addressGeneration == addressGenerations.getOrDefault(attempt.address, 0L)
                && attempt.dcGeneration == dcGenerations.getOrDefault(attempt.dcId, 0L);
    }

    private final class RefreshAttempt {

        private final HostPort address;
        private final String dcId;
        private final RedisSession session;
        private final long lifecycleGeneration;
        private final long addressGeneration;
        private final long dcGeneration;

        private RefreshAttempt(HostPort address, String dcId, RedisSession session,
                               long lifecycleGeneration, long addressGeneration, long dcGeneration) {
            this.address = address;
            this.dcId = dcId;
            this.session = session;
            this.lifecycleGeneration = lifecycleGeneration;
            this.addressGeneration = addressGeneration;
            this.dcGeneration = dcGeneration;
        }

        private void start() {
            ConfigGetFlagCommand prepareWatch = new ConfigGetFlagCommand(session, PREPARE_WATCH);
            ConfigGetFlagCommand pubsubParse = new ConfigGetFlagCommand(session, PUBSUB_PARSE);
            ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
            chain.add(prepareWatch);
            chain.add(pubsubParse);
            chain.execute().addListener(future -> finish(prepareWatch.future(), pubsubParse.future()));
        }

        private void finish(CommandFuture<Boolean> prepareWatch, CommandFuture<Boolean> pubsubParse) {
            Capability capability = null;
            boolean changed = false;
            try {
                logFailure(PREPARE_WATCH, prepareWatch);
                logFailure(PUBSUB_PARSE, pubsubParse);
                synchronized (stateLock) {
                    try {
                        if (prepareWatch.isSuccess() && pubsubParse.isSuccess() && isCurrent(this)) {
                            capability = Boolean.TRUE.equals(prepareWatch.getNow())
                                    && Boolean.TRUE.equals(pubsubParse.getNow())
                                    ? Capability.SUPPORTED : Capability.UNSUPPORTED;
                            CacheEntry previous = values.put(address, new CacheEntry(dcId, capability));
                            changed = previous == null || previous.capability != capability
                                    || !Objects.equals(previous.dcId, dcId);
                        }
                    } finally {
                        if (inflight.get(address) == this) {
                            inflight.remove(address);
                        }
                    }
                }
                if (changed) {
                    logCapabilityChange(capability);
                }
            } catch (Throwable throwable) {
                logger.warn("[refresh] failed to finish Keeper capability refresh keeper={}", address, throwable);
            }
        }

        private void logFailure(String key, CommandFuture<Boolean> future) {
            if (!future.isSuccess()) {
                logger.warn("[refresh] CONFIG GET failed keeper={}, key={}", address, key, future.cause());
            }
        }

        private void logCapabilityChange(Capability capability) {
            logger.info("[refresh] Keeper capability changed keeper={}, dc={}, capability={}",
                    address, dcId, capability);
            try {
                CatEventMonitor.DEFAULT.logEvent(CAPABILITY_EVENT, address + ":" + capability);
            } catch (Throwable monitorFailure) {
                logger.warn("[refresh] failed to record Keeper capability event keeper={}", address, monitorFailure);
            }
        }
    }

    private final class ConfigGetFlagCommand extends AbstractCommand<Boolean> {

        private final RedisSession session;
        private final String configKey;
        private final AtomicBoolean completed = new AtomicBoolean();

        private ConfigGetFlagCommand(RedisSession session, String configKey) {
            this.session = session;
            this.configKey = configKey;
        }

        @Override
        protected void doExecute() {
            try {
                session.ConfigGet(new Callbackable<String>() {
                    @Override
                    public void success(String value) {
                        complete(value, null);
                    }

                    @Override
                    public void fail(Throwable throwable) {
                        complete(null, throwable);
                    }
                }, configKey);
            } catch (Throwable throwable) {
                complete(null, throwable);
            }
        }

        private void complete(String value, Throwable throwable) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }
            if (throwable != null) {
                fail(throwable);
            } else {
                future().setSuccess(value != null && "1".equals(value.trim()));
            }
        }

        @Override
        protected void doReset() {
            completed.set(false);
        }

        @Override
        public String getName() {
            return "KeeperConfigGet-" + configKey;
        }
    }

    private static final class CacheEntry {
        private final String dcId;
        private final Capability capability;

        private CacheEntry(String dcId, Capability capability) {
            this.dcId = dcId;
            this.capability = capability;
        }
    }
}
