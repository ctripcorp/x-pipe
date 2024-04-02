package com.ctrip.xpipe.redis.console.keeper.Command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.command.RetryCommandFactory;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class SwitchMasterCommand<T> extends AbstractKeeperCommand<T>{

    private String activeIp;

    private String backupIp;

    private List<RedisTbl> keepers;

    private KeeperContainerService keeperContainerService;

    public SwitchMasterCommand(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, String activeIp, String backupIp, List<RedisTbl> keepers, KeeperContainerService keeperContainerService) {
        super(keyedObjectPool, scheduled);
        this.activeIp = activeIp;
        this.backupIp = backupIp;
        this.keepers = keepers;
        this.keeperContainerService = keeperContainerService;
    }

    @Override
    public String getName() {
        return "SwitchMasterCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        try {
            logger.info("[zyfTest][SwitchMasterCommand] start");
            if (keepers.size() != 2) {
                logger.warn("[switchMaster] keeper size is not 2, can not switch master, activeIp: {}, backupIp: {}, shardModelKeepers: {}", activeIp, backupIp, keepers);
                return;
            }
            int activeKeeperPort = -1;
            String backUpKeeperIp = null;
            for (RedisTbl keeper : keepers) {
                if (keeper.getRedisIp().equals(activeIp)) {
                    activeKeeperPort = keeper.getRedisPort();
                } else {
                    backUpKeeperIp = keeper.getRedisIp();
                }
            }

            if (activeKeeperPort == -1 || backUpKeeperIp == null || !backUpKeeperIp.equals(backupIp)) {
                logger.warn("[switchMaster]  can not find truly active keeper or backup keeper, activeIp: {}, backupIp: {}, shardModelKeepers: {}, activeKeeperPort: {}, backUpKeeperIp: {}"
                        , activeIp, backupIp, keepers, activeKeeperPort, backUpKeeperIp);
                return;
            }

            KeeperTransMeta keeperInstanceMeta = null;
            logger.info("[zyfTest][SwitchMasterCommand] start getAllKeepers");
            List<KeeperInstanceMeta> allKeepers = keeperContainerService.getAllKeepers(activeIp);
            logger.info("[zyfTest][SwitchMasterCommand] over getAllKeepers");
            for (KeeperInstanceMeta keeper : allKeepers) {
                if (keeper.getKeeperMeta().getPort() == activeKeeperPort) {
                    keeperInstanceMeta = keeper;
                    break;
                }
            }

            if (keeperInstanceMeta == null) {
                logger.warn("[switchMaster]  can not find keeper: {}:{} replId message", activeIp, activeKeeperPort);
                return;
            }
            logger.info("[zyfTest][SwitchMasterCommand] start resetKeepers");
            keeperContainerService.resetKeepers(keeperInstanceMeta);
            logger.info("[zyfTest][SwitchMasterCommand] over resetKeepers");
            RetryCommandFactory<String> commandFactory = DefaultRetryCommandFactory.retryNTimes(scheduled, 5, 1000);
            Command<String> retryInfoCommand = commandFactory.createRetryCommand(generteInfoCommand(new DefaultEndPoint(activeIp, activeKeeperPort)));
            logger.info("[zyfTest][SwitchMasterCommand] get retryInfoCommand");
            int finalActiveKeeperPort = activeKeeperPort;
            addHookAndExecute(retryInfoCommand, new Callbackable<String>() {
                @Override
                public void success(String message) {
                    logger.info("[zyfTest][SwitchMasterCommand] retryInfoCommand success");
                    if (!new InfoResultExtractor(message).getKeeperActive()) {
                        future().setSuccess();
                    }
                }

                @Override
                public void fail(Throwable throwable) {
                    logger.info("[zyfTest][SwitchMasterCommand] retryInfoCommand fail");
                    logger.error("[SwitchMasterCommand] info keeper: {}:{}", activeIp, finalActiveKeeperPort, throwable);
                }
            });
            if (retryInfoCommand.future().isSuccess()) {
                future().setSuccess();
                logger.info("[zyfTest][SwitchMasterCommand] over success");
            }
        } catch (Exception e) {
            logger.error("[SwitchMasterCommand]  switch master failed, activeIp: {}, backupIp: {}", activeIp, backupIp, e);
        }
    }

    @Override
    protected void doReset() {

    }
}
