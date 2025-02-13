package com.ctrip.xpipe.redis.console.controller.api;


import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.RedisMsg;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.impl.KeeperContainerUsedInfoMsgCollector;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.KeeperRestElectionModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_CONTAINER_IO_RATE;
import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_CONTAINER_STANDARD;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class KeeperContainerController extends AbstractConsoleController{

    @Autowired
    KeeperContainerUsedInfoAnalyzer analyzer;

    @Autowired
    ConfigService configService;

    @Autowired
    KeeperContainerUsedInfoMsgCollector keeperContainerUsedInfoMsgCollector;

    @Autowired
    KeeperContainerCheckerService keeperContainerService;

    @RequestMapping(value = "/keepercontainer/overload/info/all", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getAllReadyToMigrateKeeperContainers() {
        return analyzer.getCurrentDcReadyToMigrationKeeperContainers();
    }

    @RequestMapping(value = "/keepercontainer/info/all", method = RequestMethod.GET)
    public List<KeeperContainerUsedInfoModel> getAllKeeperContainerUsedInfoModelsList() {
        return analyzer.getCurrentDcKeeperContainerUsedInfoModelsList();
    }

    @RequestMapping(value = "/keepercontainer/redis/msg/{dc}", method = RequestMethod.GET)
    public Map<HostPort, RedisMsg> getAllDcRedisMsg(@PathVariable String dc) {
        return keeperContainerUsedInfoMsgCollector.getDcRedisMsg(dc);
    }

    @RequestMapping(value = "/keepercontainer/diskType", method = RequestMethod.POST)
    public RetMessage setDiskType(@RequestBody ConfigModel configModel) {
        try {
            configModel.setKey(KEY_KEEPER_CONTAINER_STANDARD);
            configService.setKeyKeeperContainerStandard(configModel);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepercontainer/diskType", method = RequestMethod.GET)
    public String getDiskType() {
        try {
            Map<String, String> map = new HashMap<>();
            List<ConfigModel> configs = configService.getConfigs(KEY_KEEPER_CONTAINER_STANDARD);
            for (ConfigModel configModel : configs) {
                map.put(configModel.getSubKey(), configModel.getVal());
            }
            return map.toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @RequestMapping(value = "/keepercontainer/ioRate", method = RequestMethod.POST)
    public RetMessage setIORate(@RequestBody ConfigModel configModel) {
        try {
            configModel.setKey(KEY_KEEPER_CONTAINER_IO_RATE);
            configService.setKeyKeeperContainerIoRate(configModel);
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepercontainer/ioRate", method = RequestMethod.GET)
    public RetMessage getIORate() {
        try {
            Map<String, String> map = new HashMap<>();
            List<ConfigModel> configs = configService.getConfigs(KEY_KEEPER_CONTAINER_IO_RATE);
            for (ConfigModel configModel : configs) {
                map.put(configModel.getSubKey(), configModel.getVal());
            }
            return new GenericRetMessage(0, map);
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepers/election/reset", method = RequestMethod.POST)
    public RetMessage resetElection(@RequestBody KeeperRestElectionModel model){
        try {
            keeperContainerService.resetKeeper(model.getIp(), Long.parseLong(model.getShardId()));
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepers/release/rdb", method = RequestMethod.POST)
    public RetMessage releaseRdb(@RequestBody KeeperRestElectionModel model){
        try {
            keeperContainerService.releaseRdb(model.getIp(), Integer.parseInt(model.getPort()), Long.parseLong(model.getShardId()));
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }



}
