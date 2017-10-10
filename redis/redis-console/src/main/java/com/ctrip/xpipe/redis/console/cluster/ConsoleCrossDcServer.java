package com.ctrip.xpipe.redis.console.cluster;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.xbill.DNS.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

    private int checkIntervalMilli = Integer.parseInt(System.getProperty("CROSS_DC_CHECK_INTERVAL_MILLI", "5000"));

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

                checkDnsCurrentDc();

            }

        }, checkIntervalMilli, checkIntervalMilli, TimeUnit.MILLISECONDS);
    }

    private void checkDnsCurrentDc() {

        String domain = consoleConfig.getConsoleDomain();

        List<String> cnames = null;
        try {
            cnames = lookUpCname(domain);
        } catch (TextParseException e) {
            logger.error("[checkDnsCurrentDc]" + domain, e);
        }

        if (cnames == null) {
            return;
        }

        if (cnames.size() == 0) {
            //only one dc
            setCrossDcLeader(true, "cnames size == 0");
            return;
        }

        String currentDc = FoundationService.DEFAULT.getDataCenter();
        Map<String, String> consoleCnameToDc = consoleConfig.getConsoleCnameToDc();

        for (String cname : cnames) {

            String dc = consoleCnameToDc.get(cname);
            if (currentDc.equalsIgnoreCase(dc)) {
                //is dc leader
                setCrossDcLeader(true, String.format("[good]current dc %s, cname:%s, cnametodc:%s", currentDc, cname, consoleCnameToDc));
                return;
            }
        }

        setCrossDcLeader(false, String.format("[bad]current dc %s, cname:%s, cnametodc:%s", currentDc, cnames, consoleCnameToDc));
    }

    protected List<String> lookUpCname(String domain) throws TextParseException {

        List<String> cnameRecords = new LinkedList<>();

        Record[] records = new Lookup(domain, Type.CNAME).run();

        if (records != null) {

            for (int i = 0; i < records.length; i++) {

                CNAMERecord cnameRecord = (CNAMERecord) records[i];
                Name target = cnameRecord.getTarget();
                String cname = target.toString();
                if (cname.endsWith(".")) {
                    cname = cname.substring(0, cname.length() - 1);
                }
                cnameRecords.add(cname);
            }
        }
        logger.debug("[lookUpCname]{}", cnameRecords);
        return cnameRecords;
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
            becomeLeader();
        }else if(previous && !crossDcLeader){
            logger.info("[loseLeader]{}", reason);
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

    public void setConsoleConfig(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
    }

    public void setCheckIntervalMilli(int checkIntervalMilli) {
        this.checkIntervalMilli = checkIntervalMilli;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
