package com.ctrip.xpipe.redis.keeper.health;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.observer.AbstractLifecycleObservable;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.HealthState;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.health.rule.IoHealthSettings;
import com.ctrip.xpipe.redis.keeper.health.rule.IoRuleEvaluator;
import com.ctrip.xpipe.redis.keeper.health.rule.RuleContext;
import com.ctrip.xpipe.redis.keeper.health.rule.RuleResult;
import com.ctrip.xpipe.redis.keeper.storage.health.IoTelemetry;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Container-wide TFS IO health: runs the rule set every tick and drives each keeper's state. */
@Component
public class TfsHealthChecker extends AbstractLifecycleObservable implements TopElement {

    static final String METRIC_TYPE = "keepercontainer.io.healthstate";

    @Autowired
    private KeeperContainerConfig config;

    /** Injected by KeeperContainerService; keeps the dependency one-way. */
    private Supplier<Collection<RedisKeeperServer>> keepers = Collections::emptyList;

    private ScheduledExecutorService scheduled;
    private DynamicDelayPeriodTask checkTask;
    private DynamicDelayPeriodTask metricTask;

    IoTelemetry telemetry = IoTelemetry.INSTANCE;
    LongSupplier nanoClock = System::nanoTime;
    MetricProxy metricProxy = MetricProxy.DEFAULT;

    private final Map<String, HealthState> states = new ConcurrentHashMap<>();
    /** Last state we know reached zk (confirmed by a successful resetElection). Drives retry. */
    private final Map<String, HealthState> publishedStates = new ConcurrentHashMap<>();

    IoRuleEvaluator evaluator = IoRuleEvaluator.defaults();

    /** Metric dimensions; filled in doInitialize, empty in tests. */
    private String mode = "";
    private String az = "";
    private String group = "";

    public void bindKeepers(Supplier<Collection<RedisKeeperServer>> keepers) {
        this.keepers = keepers;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.mode = nullToEmpty(config.getMode());
        this.az = nullToEmpty(safeFoundationValue(FoundationService.DEFAULT::getRegion));
        this.group = nullToEmpty(safeFoundationValue(FoundationService.DEFAULT::getGroupId));
        String name = getClass().getSimpleName();
        this.scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create(name));
        this.checkTask = new DynamicDelayPeriodTask(name + "-check", this::check,
                config::ioHealthCheckIntervalMill, scheduled);
        this.metricTask = new DynamicDelayPeriodTask(name + "-metric", this::reportMetric,
                config::ioHealthMetricIntervalMill, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        checkTask.start();
        metricTask.start();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        checkTask.stop();
        metricTask.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        scheduled.shutdown();
    }

    public HealthState state(String tenant) {
        return states.getOrDefault(tenant, HealthState.HEALTHY);
    }

    public void check() {
        try {
            check(nanoClock.getAsLong());
        } catch (Throwable th) {
            logger.info("[check] fail", th);
        }
    }

    void check(long nowNanos) {
        // apply first: on a non-TFS container this switches recording off in the telemetry
        IoHealthSettings settings = IoHealthSettings.from(config);
        telemetry.applySettings(settings);
        // BM containers share the same IO pool; their local-disk health belongs to DiskHealthChecker
        if (!settings.enabled()) {
            return;
        }
        if (!warmedUp(nowNanos)) {
            return;
        }

        RuleResult result = evaluator.evaluate(
                new RuleContext(telemetry.snapshot(nowNanos), settings.rules(), nowNanos));

        Set<String> seen = new HashSet<>();
        for (RedisKeeperServer keeper : keepers.get()) {
            String tenant = tenantOf(keeper);
            if (tenant == null || !seen.add(tenant)) {
                continue;
            }
            HealthState current = states.computeIfAbsent(tenant, t -> seedState(keeper));
            HealthState next = next(current, result.isAnomalous(tenant));
            if (next != current) {
                states.put(tenant, next);
                logger.info("[check][{}]{} -> {}, triggered={}", tenant, current, next, result.triggered());
            }
            // publish whenever zk lags the decided state, so a throttled/failed reset self-heals here
            if (needsPublish(next) && next != publishedStates.get(tenant)) {
                onChange(keeper, tenant, next);
            }
        }
        states.keySet().retainAll(seen);
        publishedStates.keySet().retainAll(seen);
    }

    /** DOWN and RECOVERED_PENDING are the states the metaserver must see; HEALTHY/SICK are not published. */
    private static boolean needsPublish(HealthState state) {
        return state == HealthState.DOWN || state == HealthState.RECOVERED_PENDING;
    }

    /** Called by KeeperContainerService once a resetElection succeeded, so we stop retrying this state. */
    public void onElectionReset(String tenant) {
        HealthState decided = states.get(tenant);
        if (decided != null) {
            publishedStates.put(tenant, decided);
        }
    }

    private boolean warmedUp(long nowNanos) {
        return telemetry.recordedCount() >= config.ioHealthMinRecords()
                && nowNanos - telemetry.startedNanos() >= TimeUnit.MILLISECONDS.toNanos(config.ioHealthWarmupMill());
    }

    private HealthState seedState(RedisKeeperServer keeper) {
        String healthState = null;
        try {
            KeeperMeta meta = keeper.getCurrentKeeperMeta();
            healthState = meta == null ? null : meta.getHealthState();
        } catch (Throwable t) {
            logger.warn("[seedState] keeper {} meta unavailable", keeper, t);
        }
        return HealthState.parse(healthState);
    }

    static HealthState next(HealthState current, boolean anomalous) {
        switch (current) {
            case HEALTHY:
                return anomalous ? HealthState.SICK : HealthState.HEALTHY;
            case SICK:
                return anomalous ? HealthState.DOWN : HealthState.HEALTHY;
            case DOWN:
                return anomalous ? HealthState.DOWN : HealthState.RECOVERED_PENDING;
            default:
                // RECOVERED_PENDING: only the API may bring it back into election
                return HealthState.RECOVERED_PENDING;
        }
    }

    private void onChange(RedisKeeperServer keeper, String tenant, HealthState next) {
        // only DOWN / RECOVERED_PENDING reach here (see needsPublish); SICK is metric-only and
        // HEALTHY from SICK never touched zk, so neither is published
        KeeperMeta meta = keeper.getCurrentKeeperMeta();
        if (meta != null) {
            meta.setHealthState(next.name());
        }
        notifyObservers(tenant);
    }

    /** One data point for the container: 0 when healthy, 1 otherwise; the tag carries the worst state. */
    void reportMetric() {
        HealthState state = containerState();
        MetricData data = new MetricData(METRIC_TYPE);
        data.setValue(state == HealthState.HEALTHY ? 0 : 1);
        data.setTimestampMilli(System.currentTimeMillis());
        data.addTag("state", state.name());
        data.addTag("mode", mode);
        data.addTag("az", az);
        data.addTag("group", group);
        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Throwable t) {
            logger.warn("[reportMetric] write failed", t);
        }
    }

    /** Worst state among the tracked keepers; HEALTHY when none is abnormal. */
    private HealthState containerState() {
        HealthState worst = HealthState.HEALTHY;
        for (RedisKeeperServer keeper : keepers.get()) {
            String tenant = tenantOf(keeper);
            if (tenant == null) {
                continue;
            }
            HealthState state = state(tenant);
            if (severity(state) > severity(worst)) {
                worst = state;
            }
        }
        return worst;
    }

    private static int severity(HealthState state) {
        switch (state) {
            case DOWN: return 3;
            case RECOVERED_PENDING: return 2;
            case SICK: return 1;
            default: return 0;
        }
    }

    private static String tenantOf(RedisKeeperServer keeper) {
        return keeper == null || keeper.getReplId() == null ? null : keeper.getReplId().toString();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String safeFoundationValue(Supplier<String> getter) {
        try {
            return getter.get();
        } catch (Throwable t) {
            return "";
        }
    }
}
