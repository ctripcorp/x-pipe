package com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class GetSentinelMonitorNamesCommand extends AbstractCommand<Set<String> > {

    private DcService dcService;

    private ClusterDao clusterDao;

    private ShardDao shardDao;

    private Executor executors;

    private RetryCommandFactory factory;

    private Map<String, Set<String> > dcToClusterNames;

    Map<String, Set<String> > clusterToMonitorNames;

    private static final int COMMAND_RETRY_TIMES = 3;
    private static final int COMMAND_RETRY_DELAY_MILLI = 1000;

    public GetSentinelMonitorNamesCommand(DcService dcService, ClusterDao clusterDao, ShardDao shardDao,
                                          Executor executors, ScheduledExecutorService scheduled) {
        this.dcService = dcService;
        this.clusterDao = clusterDao;
        this.shardDao = shardDao;
        this.executors = executors;
        this.factory = DefaultRetryCommandFactory.retryNTimes(scheduled, COMMAND_RETRY_TIMES, COMMAND_RETRY_DELAY_MILLI);
    }

    @Override
    protected void doExecute() throws Exception {
        List<DcTbl> dcTbls = dcService.findAllDcs();
        dcToClusterNames = new ConcurrentHashMap<>(dcTbls.size());
        clusterToMonitorNames = null;

        ParallelCommandChain commandChain = new ParallelCommandChain();
        dcTbls.forEach(dcTbl -> commandChain.add(retryCommand(new GetDcClusterNamesCommand(dcTbl.getId(), dcTbl.getDcName()))));
        commandChain.add(retryCommand(new GetClusterWithShardMonitorNamesCommand()));

        commandChain.execute(executors).get();
        future().setSuccess(format());
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    private <T> Command<T> retryCommand(Command<T> command) {
        return factory.createRetryCommand(command);
    }

    private Set<String> format() {
        Set<String> realMonitorNames = new HashSet<>();
        dcToClusterNames.forEach((dcName, clusterNames) -> clusterNames.forEach(clusterName -> {
            if (!clusterToMonitorNames.containsKey(clusterName)) return;
            clusterToMonitorNames.get(clusterName).forEach(monitorName ->
                    realMonitorNames.add(SentinelUtil.getSentinelMonitorName(clusterName, monitorName, dcName)));
        }));

        return realMonitorNames;
    }

    class GetDcClusterNamesCommand extends AbstractCommand<Void> {

        private long dcId;

        private String dcName;

        public GetDcClusterNamesCommand(long dcId, String dcName) {
            this.dcId = dcId;
            this.dcName = dcName;
        }

        @Override
        protected void doExecute() throws Exception {
            List<ClusterTbl> clusterTbls = clusterDao.findAllClusterNamesByDcId(dcId);
            dcToClusterNames.put(dcName, format(clusterTbls));
            future().setSuccess();
        }

        @Override
        protected void doReset() {
            // do nothing
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        public Set<String> format(List<ClusterTbl> clusterTbls) {
            return clusterTbls.stream().map(ClusterTbl::getClusterName).collect(Collectors.toSet());
        }

    }

    class GetClusterWithShardMonitorNamesCommand extends AbstractCommand<Void> {

        @Override
        protected void doExecute() throws Exception {
            List<ShardTbl> shardTbls = shardDao.findAllClusterShardMonitorName();
            clusterToMonitorNames = format(shardTbls);
            future().setSuccess();
        }

        @Override
        protected void doReset() {
            // do nothing
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        private Map<String, Set<String> > format(List<ShardTbl> shardTbls) {
            Map<String, Set<String> > clusterMonitorNamesMap = new HashMap<>();
            shardTbls.forEach(shardTbl -> {
                if (StringUtil.isEmpty(shardTbl.getSetinelMonitorName())) return;
                Set<String> monitors = MapUtils.getOrCreate(clusterMonitorNamesMap, shardTbl.getClusterInfo().getClusterName(), HashSet::new);
                monitors.add(shardTbl.getSetinelMonitorName());
            });

            return clusterMonitorNamesMap;
        }
    }

}
