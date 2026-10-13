package com.ctrip.xpipe.redis.console.controller.api;


import com.ctrip.xpipe.redis.checker.controller.result.GenericRetMessage;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.entity.DcCheckerReportMsg;
import com.ctrip.xpipe.redis.console.keeper.impl.KeeperContainerUsedInfoMsgCollector;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.KeeperRestElectionModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot;
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

    @Autowired
    CurrentDcKeeperContainerAddressService keeperContainerAddressService;

    @Autowired
    CrossDcKeeperContainerAddressService crossDcKeeperContainerAddressService;

    @RequestMapping(value = "/keepercontainer/overload/info/all", method = RequestMethod.GET)
    public List<MigrationKeeperContainerDetailModel> getAllReadyToMigrateKeeperContainers() {
        return analyzer.getCurrentDcReadyToMigrationKeeperContainers();
    }

    @RequestMapping(value = "/keepercontainer/info/all", method = RequestMethod.GET)
    public List<KeeperContainerUsedInfoModel> getAllKeeperContainerUsedInfoModelsList() {
        return analyzer.getCurrentDcKeeperContainerUsedInfoModelsList();
    }

    @RequestMapping(value = "/keepercontainer/redis/msg", method = RequestMethod.GET)
    public DcCheckerReportMsg getCurrentDcRedisMsg() {
        return keeperContainerUsedInfoMsgCollector.getCurrentDcRedisMasterMsg();
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
            keeperContainerService.resetKeeper(model.getDcName(), model.getIp(), model.getHostname(), Long.parseLong(model.getShardId()));
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepers/release/rdb", method = RequestMethod.POST)
    public RetMessage releaseRdb(@RequestBody KeeperRestElectionModel model){
        try {
            keeperContainerService.releaseRdb(model.getDcName(), model.getIp(), model.getHostname(), Integer.parseInt(model.getPort()), Long.parseLong(model.getShardId()));
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepercontainer/address/{hostname}", method = RequestMethod.GET)
    public RetMessage resolveKeeperContainerIp(@PathVariable String hostname) {
        try {
            String ip = keeperContainerAddressService.getIpByHostname(hostname);
            if (ip != null) {
                return RetMessage.createSuccessMessage(ip);
            } else {
                return RetMessage.createFailMessage("hostname not found: " + hostname);
            }
        } catch (Exception e) {
            return RetMessage.createFailMessage(e.getMessage());
        }
    }

    @RequestMapping(value = "/keepercontainer/address/all", method = RequestMethod.GET)
    public Map<String, String> getAllKeeperContainerHostnameToIpMap() {
        return keeperContainerAddressService.getAllHostnameToIp();
    }

    /** 所有机房完整地址快照，按 DC 分组；远端数据来自本地周期快照缓存。 */
    @RequestMapping(value = "/keepercontainer/address/snapshot/global", method = RequestMethod.GET)
    public Map<String, KeeperContainerAddressSnapshot> getGlobalKeeperContainerAddressSnapshot() {
        return crossDcKeeperContainerAddressService.getAllAddressSnapshots();
    }

    /**
     * 本机房地址快照（存活正向 / tombstone + 剩余毫秒 / 存活反向），供远端机房 console 周期拉取（spec §8.3）。
     * 字面路径优先于 {@code /address/{hostname}} 匹配。只读内存；每个远端 console 实例每个周期调一次，不打入口日志。
     */
    @RequestMapping(value = "/keepercontainer/address/snapshot", method = RequestMethod.GET)
    public KeeperContainerAddressSnapshot getKeeperContainerAddressSnapshot() {
        return keeperContainerAddressService.getAddressSnapshot();
    }

}
