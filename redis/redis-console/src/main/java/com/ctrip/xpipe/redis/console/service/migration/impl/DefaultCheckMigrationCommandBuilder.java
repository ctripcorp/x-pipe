package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandRetryWrapper;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.job.retry.RetryCondition;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.migration.CheckMigrationCommandBuilder;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.exception.NoResourceException;
import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

public class DefaultCheckMigrationCommandBuilder extends AbstractService implements CheckMigrationCommandBuilder {

    private ScheduledExecutorService scheduled;

    private DcService dcService;

    private ClusterService clusterService;

    private OuterClientService outerClientService;

    private ConsoleConfig consoleConfig;

    public DefaultCheckMigrationCommandBuilder(ScheduledExecutorService scheduled, DcService dcService, ClusterService clusterService,
                                               OuterClientService outerClientService, ConsoleConfig consoleConfig) {
        this.scheduled = scheduled;
        this.dcService = dcService;
        this.clusterService = clusterService;
        this.outerClientService = outerClientService;
        this.consoleConfig = consoleConfig;
    }

    @Override
    public Command<RetMessage> checkCommand(CHECK_MIGRATION_SYSTEM_STEP step) {
        Pair<String, String> clusterShard = consoleConfig.getClusterShardForMigrationSysCheck();
        String clusterName = clusterShard.getKey(), shardName = clusterShard.getValue();
        switch (step) {
            case CHECK_DATA_BASE:
                return checkDatabaseRetryCommand(clusterName);
            case CHECK_OUTER_CLIENT:
                return new CheckOuterClientCommand(clusterName);
            case CHECK_METASERVER:
                Map<String, String> metaServers = consoleConfig.getMetaservers();
                return new CheckMetaServerCommand(metaServers, clusterName, shardName);
        }
        return null;
    }

    private Command<RetMessage> checkDatabaseRetryCommand(String clusterName) {
        RetryCondition<RetMessage> retryCondition = new RetryCondition.AbstractRetryCondition<RetMessage>() {
            @Override
            public boolean isSatisfied(RetMessage retMessage) {
                return retMessage.getState() == RetMessage.SUCCESS_STATE;
            }

            @Override
            public boolean isExceptionExpected(Throwable th) {
                return false;
            }
        };
        return CommandRetryWrapper.buildCountRetry(3, retryCondition, new CheckDatabaseCommand(clusterName), scheduled);
    }

    private abstract class AbstractCheckMigrationSystemCommand<T> extends AbstractCommand<RetMessage> {

        @Override
        protected void doExecute() throws Exception {
            T response = null;
            try {
                response = getResponse();
            } catch (Exception e) {
                future().setFailure(e);
                return;
            }
            if(response == null) {
                future().setFailure(new NoResourceException("no response from source"));
                return;
            }
            try {
                future().setSuccess(validate(response));
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        protected abstract T getResponse() throws Exception;

        protected abstract RetMessage validate(T response) throws Exception;

    }

    public class CheckDatabaseCommand extends AbstractCheckMigrationSystemCommand<ClusterTbl> {

        private String clusterName;

        public CheckDatabaseCommand(String clusterName) {
            this.clusterName = clusterName;
        }

        @Override
        protected ClusterTbl getResponse() {
            return clusterService.find(clusterName);
        }

        @Override
        protected RetMessage validate(ClusterTbl response) throws Exception {
            if (response.getClusterName().equals(clusterName)) {
                return RetMessage.createSuccessMessage();
            }
            return RetMessage.createFailMessage(String.format("cluster name not matched from database as: %s",
                    response.getClusterName()));
        }
    }

    public class CheckOuterClientCommand extends AbstractCheckMigrationSystemCommand<OuterClientService.ClusterInfo> {

        private String clusterName;

        public CheckOuterClientCommand(String clusterName) {
            this.clusterName = clusterName;
        }

        @Override
        protected OuterClientService.ClusterInfo getResponse() throws Exception {
            return outerClientService.getClusterInfo(clusterName);
        }

        @Override
        protected RetMessage validate(OuterClientService.ClusterInfo response) {
            if(clusterName.equals(response.getName())) {
                return RetMessage.createSuccessMessage();
            }
            return RetMessage.createFailMessage(String.format("cluster name not matched from outer client as: %s",
                    response.getName()));
        }
    }

    public class CheckMetaServerCommand extends AbstractCheckMigrationSystemCommand<List<String>> {

        private Map<String, String> metaServerAddresses;

        private List<String> targetMetaServers = Lists.newArrayList();

        private String clusterName, shardName;

        public CheckMetaServerCommand(Map<String, String> metaservers, String clusterName, String shardName) {
            this.metaServerAddresses = metaservers;
            this.clusterName = clusterName;
            this.shardName = shardName;
        }

        @Override
        protected List<String> getResponse() throws Exception {
            List<DcTbl> dcTbls = dcService.findClusterRelatedDc(clusterName);
            for(DcTbl dcTbl : dcTbls) {
                if(metaServerAddresses.containsKey(dcTbl.getDcName())) {
                    targetMetaServers.add(metaServerAddresses.get(dcTbl.getDcName()));
                }
            }
            logger.info("[CheckMetaServerCommand][target meta-servers]{}", targetMetaServers);
            Set<String> result = Sets.newHashSet();

            for(String metaServerAddress : targetMetaServers) {
                String activeKeeperPath = META_SERVER_SERVICE.GET_ACTIVE_KEEPER.getRealPath(metaServerAddress);
                try {
                    KeeperMeta keeperMeta = restTemplate.getForObject(activeKeeperPath, KeeperMeta.class, clusterName, shardName);
                    if (keeperMeta != null) {
                        result.add(metaServerAddress);
                    }
                } catch (Exception e) {
                    logger.error("[CheckMetaServerCommand][{}][{}]", clusterName, metaServerAddress, e);
//                    throw new NoResponseException(String.format("MetaServer: %s", metaServerAddress), e);
                }
            }
            return Lists.newArrayList(result);
        }

        @Override
        protected RetMessage validate(List<String> response) {
            if(response.size() == targetMetaServers.size()) {
                return RetMessage.createSuccessMessage();
            } else if(response.isEmpty()) {
                return RetMessage.createFailMessage("All MetaServers Down");
            }
            List<String> problemMetaServers = Lists.newArrayList(targetMetaServers);

            problemMetaServers.removeAll(response);
            StringBuilder sb = new StringBuilder("Non-Responsed Metaservers: ");
            for(String addr : problemMetaServers) {
                sb.append(addr).append(",");
            }
            return RetMessage.createWarningMessage(sb.toString());
        }

    }
}
