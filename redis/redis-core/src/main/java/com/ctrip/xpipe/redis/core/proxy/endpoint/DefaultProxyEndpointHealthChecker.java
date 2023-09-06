package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class DefaultProxyEndpointHealthChecker implements ProxyEndpointHealthChecker {

    protected static long DEFAULT_DROP_ENDPOINT_INTERVAL_MILLI = Long.parseLong(System.getProperty("endpoint.health.check.drop.interval", "600000"));

    protected static int ENDPOINT_HEALTH_CHECK_INTERVAL = Integer.parseInt(System.getProperty("endpoint.health.check.interval", "1000"));

    private static final String MONITOR_TYPE = "Endpoint.Health.State.Change";

    private static final Logger logger = LoggerFactory.getLogger(DefaultProxyEndpointHealthChecker.class);

    private ScheduledExecutorService scheduled;

    private EventMonitor eventMonitor = EventMonitor.DEFAULT;

    private Map<Endpoint, EndpointHealthStatus> allHealthStatus = Maps.newConcurrentMap();

    public DefaultProxyEndpointHealthChecker(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
    }

    @Override
    public boolean checkConnectivity(ProxyEndpoint endpoint) {
        try {
            // will not check non-proxy endpoints
            if (!endpoint.isProxyProtocolSupported()) {
                return true;
            }
            if(!allHealthStatus.containsKey(endpoint)) {
                EndpointHealthStatus status = new EndpointHealthStatus(endpoint);
                allHealthStatus.put(endpoint, status);
                status.start();
            }
            return allHealthStatus.get(endpoint).isHealthy();
        } catch (Exception e) {
            logger.error("[checkConnectivity]", e);
        }
        return false;
    }

    @Override
    public boolean resetIfNeed(ProxyEndpoint endpoint) {
        if (!allHealthStatus.containsKey(endpoint)) return false;
        EndpointHealthStatus status = allHealthStatus.get(endpoint);

        long currentTime = System.currentTimeMillis();
        if(currentTime - status.getLastHealthyTimeMilli() >= DEFAULT_DROP_ENDPOINT_INTERVAL_MILLI) {
            status = allHealthStatus.remove(endpoint);
            try {
                status.stop();
                return true;
            } catch (Throwable th) {
                logger.info("[resetIfNeed][reset fail]", th);
            }
        }

        return false;
    }

    @VisibleForTesting
    protected Map<Endpoint, EndpointHealthStatus> getAllHealthStatus() {
        return allHealthStatus;
    }


    @VisibleForTesting
    protected final class EndpointHealthStatus extends AbstractStartStoppable implements Startable, Stoppable {

        private AtomicReference<EndpointHealthState> healthState = new AtomicReference<>(EndpointHealthState.UNKNOWN);

        private volatile boolean isHealthy = true;

        private volatile long lastHealthyTimeMilli = System.currentTimeMillis();

        private Endpoint endpoint;

        private AtomicReference<TcpPortCheckCommand> runningCmd;

        private ScheduledFuture future;

        protected EndpointHealthStatus(Endpoint endpoint) {
            this.endpoint = endpoint;
            this.runningCmd = new AtomicReference<>();
        }

        @Override
        protected void doStart() throws Exception {
            future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    check();
                }
            }, ENDPOINT_HEALTH_CHECK_INTERVAL, ENDPOINT_HEALTH_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void doStop() throws Exception {
            future.cancel(true);
            future = null;
        }

        public boolean isHealthy() {
            return isHealthy;
        }

        public EndpointHealthState getHealthState() {
            return healthState.get();
        }

        private synchronized void setHealthState(EndpointHealthState state) {
            EndpointHealthState previous = healthState.getAndSet(state);
            EndpointHealthState current = state;

            if(EndpointHealthState.HEALTHY.equals(state)) {
                lastHealthyTimeMilli = System.currentTimeMillis();
                isHealthy = true;
            } else if(EndpointHealthState.UNHEALTHY.equals(state)) {
                isHealthy = false;
            }

            if(!previous.equals(current)) {
                String message = String.format("%s -> %s, %s", previous.name(), current.name(), endpoint.toString());
                logger.info("[setHealthState][endpoint-state-change] {}", message);
                eventMonitor.logEvent(MONITOR_TYPE, message);
            }
        }

        @VisibleForTesting
        protected void check() {
            if (null != runningCmd.get() && !runningCmd.get().future().isDone()) {
                logger.debug("[check][skip for last check running]");
                return;
            }

            TcpPortCheckCommand command = new TcpPortCheckCommand(endpoint.getHost(), endpoint.getPort());
            command.execute().addListener(new CommandFutureListener<Boolean>() {
                @Override
                public void operationComplete(CommandFuture<Boolean> future) {
                    try {
                        if (future.isSuccess() && future.get()) {
                            setHealthState(healthState.get().afterSuccess());
                            return;
                        }
                    } catch (Throwable th) {
                        logger.error("[check][operationComplete]", th);
                    }
                    setHealthState(healthState.get().afterFail());
                }
            });
            runningCmd.set(command);
        }

        @VisibleForTesting
        protected CommandFuture<Boolean> getCurrentCheckFuture() {
            if (null == runningCmd.get()) return null;
            return runningCmd.get().future();
        }

        private long getLastHealthyTimeMilli() {
            return lastHealthyTimeMilli;
        }

        @Override
        public String toString() {
            return "EndpointHealthStatus{" +
                    "healthState=" + healthState +
                    ", isHealthy=" + isHealthy +
                    ", lastHealthyTimeMilli=" + DateTimeUtils.timeAsString(lastHealthyTimeMilli) +
                    ", endpoint=" + endpoint +
                    '}';
        }
    }

    enum EndpointHealthState {

        UNKNOWN{
            @Override
            protected EndpointHealthState afterSuccess() {
                return HEALTHY;
            }

            @Override
            protected EndpointHealthState afterFail() {
                return FAIL_ONCE;
            }
        }, SUCCESS_ONCE {

            @Override
            protected EndpointHealthState afterSuccess() {
                return HEALTHY;
            }

            @Override
            protected EndpointHealthState afterFail() {
                return FAIL_ONCE;
            }
        }, HEALTHY {

            @Override
            protected EndpointHealthState afterSuccess() {
                return HEALTHY;
            }

            @Override
            protected EndpointHealthState afterFail() {
                return FAIL_ONCE;
            }
        }, FAIL_ONCE {

            @Override
            protected EndpointHealthState afterSuccess() {
                return HEALTHY;
            }

            @Override
            protected EndpointHealthState afterFail() {
                return FAIL_TWICE;
            }
        }, FAIL_TWICE {

            @Override
            protected EndpointHealthState afterSuccess() {
                return SUCCESS_ONCE;
            }

            @Override
            protected EndpointHealthState afterFail() {
                return UNHEALTHY;
            }
        }, UNHEALTHY {

            @Override
            protected EndpointHealthState afterSuccess() {
                return SUCCESS_ONCE;
            }

            @Override
            protected EndpointHealthState afterFail() {
                return UNHEALTHY;
            }
        };

        protected abstract EndpointHealthState afterSuccess();

        protected abstract EndpointHealthState afterFail();
    }
}
