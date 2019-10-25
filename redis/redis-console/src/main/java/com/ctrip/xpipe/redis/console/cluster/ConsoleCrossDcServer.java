package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.netty.TcpPortCheckCommand;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.monitor.BaseInstantaneousMetric;
import com.ctrip.xpipe.redis.core.monitor.InstantaneousCounterMetric;
import com.ctrip.xpipe.redis.core.monitor.InstantaneousMetric;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 14, 2017
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class ConsoleCrossDcServer extends AbstractStartStoppable implements CrossDcClusterServer, LeaderAware, ApplicationContextAware{

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ConsoleLeaderElector consoleLeaderElector;

    private int checkIntervalMilli = Integer.parseInt(System.getProperty("CROSS_DC_CHECK_INTERVAL_MILLI", "600000"));

    private int startIntervalMilli = Integer.parseInt(System.getProperty("CROSS_DC_CHECK_INTERVAL_MILLI", "5000"));

    private volatile boolean crossDcLeader = false;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1, XpipeThreadFactory.create("CrossDcServer"));

    private ScheduledFuture future;

    private ApplicationContext applicationContext;

    @Override
    public boolean amILeader() {
        return crossDcLeader;
    }


    @Override
    protected void doStart() throws Exception {

        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {

                checkDataBaseCurrentDc();

            }

        }, startIntervalMilli, checkIntervalMilli, TimeUnit.MILLISECONDS);
    }

    protected void checkDataBaseCurrentDc() {
        try {
            String domainName = consoleConfig.getDatabaseDomainName();
            check(domainName);
        } catch (Exception e) {
            logger.error("[checkDataBaseCurrentDc]", e);
        }
    }

    private void check(String domainName) {
        try {
            InetAddress ipAddress = InetAddress.getByName(domainName);
            Map<String, String> ipDcMapping = consoleConfig.getDatabaseIpAddresses();
            String dcName = ipDcMapping.get(ipAddress.getHostAddress());
            triggerElection(dcName);
        } catch (Exception e) {
            logger.error("[check]Invalid database domain name", e);
        }

    }

    private void triggerElection(String dcName) {
        if(!consoleLeaderElector.amILeader()) {
            setCrossDcLeader(false, "[triggerElection]not site leader, quit for cross-site leader election");
            return;
        }
        boolean crossDcLeader = FoundationService.DEFAULT.getDataCenter().equalsIgnoreCase(dcName);
        setCrossDcLeader(crossDcLeader, String.format("[result] Database domain name: %s", dcName));
    }

    @Override
    protected void doStop() throws Exception {

        if (future != null) {
            logger.info("[doStop][cancel future]{}", future);
            future.cancel(true);
        }
    }


    @Override
    public List<String> getAllServers() {
        return new LinkedList<>();
    }

    @Override
    public void isleader() {
        try {
            //become dc leader
            start();
        } catch (Exception e) {
            logger.error("[isCrossDcLeader]", e);
        }

    }

    @Override
    public void notLeader() {
        try {
            stop();
            setCrossDcLeader(false, "lose cluster leader");
        } catch (Exception e) {
            logger.error("[isCrossDcLeader]", e);
        }
    }

    public synchronized void setCrossDcLeader(boolean crossDcLeader, String reason) {

        if(!isStarted() && crossDcLeader){
            logger.info("[setCrossDcLeader][fail, stopped]{}, {}", crossDcLeader, reason);
            return;
        }

        boolean previous = this.crossDcLeader;

        this.crossDcLeader = crossDcLeader;

        if(!previous && crossDcLeader){
            logger.info("[becomeLeader]{}", reason);
            EventMonitor.DEFAULT.logEvent("XPIPE.LEADER.CHANGE", "BECOME.LEADER");
            becomeLeader();
        }else if(previous && !crossDcLeader){
            logger.info("[loseLeader]{}", reason);
            EventMonitor.DEFAULT.logEvent("XPIPE.LEADER.CHANGE", "LOSE.LEADER");
            loseLeader();
        }
    }

    private void loseLeader() {

        logger.info("[loseLeader]");

        if(applicationContext != null){
            Map<String, CrossDcLeaderAware> beansOfType = applicationContext.getBeansOfType(CrossDcLeaderAware.class);

            beansOfType.forEach((name, dcLeaderAware) -> {
                try{
                    logger.info("[loseLeader]{}", name);
                    dcLeaderAware.notCrossDcLeader();
                }catch (Exception e){
                    logger.error("[loseLeader]" + dcLeaderAware, e);
                }
            });
        }

    }

    private void becomeLeader() {

        logger.info("[becomeLeader]");
        if(applicationContext != null){

            Map<String, CrossDcLeaderAware> beansOfType = applicationContext.getBeansOfType(CrossDcLeaderAware.class);
            beansOfType.forEach((name, dcLeaderAware) -> {
                try {
                    logger.info("[becomeLeader]{}", name);
                    dcLeaderAware.isCrossDcLeader();
                }catch (Exception e){
                    logger.error("[becomeLeader]" + dcLeaderAware, e);
                }
            });
        }
    }


    public void setCheckIntervalMilli(int checkIntervalMilli) {
        this.checkIntervalMilli = checkIntervalMilli;
    }

    @VisibleForTesting
    protected ConsoleCrossDcServer setConsoleLeaderElector(ConsoleLeaderElector consoleLeaderElector) {
        this.consoleLeaderElector = consoleLeaderElector;
        return this;
    }

    @VisibleForTesting
    protected ConsoleCrossDcServer setConsoleConfig(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
        return this;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @VisibleForTesting
    protected ConsoleCrossDcServer setStartIntervalMilli(int startIntervalMilli) {
        this.startIntervalMilli = startIntervalMilli;
        return this;
    }
}
