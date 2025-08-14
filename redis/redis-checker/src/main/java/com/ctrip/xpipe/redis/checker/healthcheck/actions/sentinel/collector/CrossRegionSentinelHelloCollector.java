package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.CheckWrongSentinels4BackupDc;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.SentinelHelloCollectContext;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component("crossRegionSentinelHelloCollector")
public class CrossRegionSentinelHelloCollector extends DefaultSentinelHelloCollector {

    protected static final Logger logger = LoggerFactory.getLogger(CrossRegionSentinelHelloCollector.class);

    @Autowired
    protected MetaCache metaCache;

    @Override
    protected SentinelHelloCollectorCommand getCommand(SentinelActionContext context) {
        return new SentinelHelloCollectorCommand4CrossRegion(context);
    }

    public class SentinelHelloCollectorCommand4CrossRegion extends SentinelHelloCollectorCommand {
        private Set<HostPort> allSentinels;

        public SentinelHelloCollectorCommand4CrossRegion(SentinelActionContext context) {
            super(context);
            allSentinels = getAllSentinels();
        }

        private Set<HostPort> getAllSentinels() {
            return metaCache.getAllSentinels();
        }

        @Override
        protected void addCommand2Chain(SequenceCommandChain chain, SentinelHelloCollectContext context) {
            chain.add(new CheckWrongSentinels4BackupDc(context, allSentinels));
        }
    }
}