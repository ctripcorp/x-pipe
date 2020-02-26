package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.election.CrossDcLeaderElectionAction;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 14, 2017
 */
@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class ConsoleCrossDcServer extends AbstractStartStoppable implements CrossDcClusterServer, LeaderAware, Observer, ApplicationContextAware{

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ConsoleLeaderElector consoleLeaderElector;

    @Autowired
    private CrossDcLeaderElectionAction electionAction;

    private volatile boolean crossDcLeader = false;

    private ApplicationContext applicationContext;

    @Override
    public boolean amILeader() {
        return crossDcLeader;
    }


    @Override
    protected void doStart() throws Exception {
        electionAction.addObserver(this);
        electionAction.start();
    }

    @Override
    public void update(Object crossDcLeader, Observable observable) {
        triggerElection((String) crossDcLeader);
    }

    private void triggerElection(String dcName) {
        if(!consoleLeaderElector.amILeader()) {
            setCrossDcLeader(false, "[triggerElection]not site leader, quit for cross-site leader election");
            return;
        }
        boolean crossDcLeader = FoundationService.DEFAULT.getDataCenter().equalsIgnoreCase(dcName);
        setCrossDcLeader(crossDcLeader, String.format("[result] cross dc leader set to %s by elect", dcName));
    }

    @Override
    protected void doStop() throws Exception {
        electionAction.removeObserver(this);
        electionAction.stop();
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

}
