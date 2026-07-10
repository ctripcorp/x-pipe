package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.BeaconCheckConfigRequest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ClusterConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.BeaconCheckConfigService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ChangeConfig extends AbstractConsoleController{

    @Autowired
    private ConfigService configService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private BeaconCheckConfigService beaconCheckConfigService;

    @Autowired
    private BeaconMetaService beaconMetaService;

    @RequestMapping(value = "/config/sentinel_auto_process/start", method = RequestMethod.POST)
    public void startSentinelAutoProcess(HttpServletRequest request,
                                         @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.startSentinelAutoProcess(config);
    }

    @RequestMapping(value = "/config/sentinel_auto_process/stop", method = RequestMethod.POST)
    public void stopSentinelAutoProcess(HttpServletRequest request,
                                        @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.stopSentinelAutoProcess(config, consoleConfig.getConfigDefaultRestoreHours());
    }

    @RequestMapping(value = "/config/alert_system/start", method = RequestMethod.POST)
    public void startAlertSystem(HttpServletRequest request,
                                 @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.startAlertSystem(config);
    }

    @RequestMapping(value = "/config/alert_system/stop", method = RequestMethod.POST)
    public void stopAlertSystem(HttpServletRequest request,
                                @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.stopAlertSystem(config, consoleConfig.getConfigDefaultRestoreHours());
    }

    @RequestMapping(value = "/config/alert_system/stop/{hours}", method = RequestMethod.POST)
    public void stopAlertSystem(HttpServletRequest request, @PathVariable int hours,
                                @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        int defaultHours = consoleConfig.getConfigDefaultRestoreHours();
        hours = hours > defaultHours || hours < 1 ? defaultHours : hours;
        configService.stopAlertSystem(config, hours);
    }

    @RequestMapping(value = "/config/ignore/migration/system/availability/{ignore}", method = RequestMethod.POST)
    public void setIgnoreMigrationSystemAvailOrNot(HttpServletRequest request, @PathVariable boolean ignore) throws DalException {
        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
        logger.info("[setIgnoreMigrationSystemAvailOrNot][{}] ignore: {}", sourceIp, ignore);
        configService.doIgnoreMigrationSystemAvailability(ignore);
    }

    @RequestMapping(value = "/config/allow/migration/auto/{allow}", method = RequestMethod.POST)
    public void setAllowAutoMigration(HttpServletRequest request, @PathVariable boolean allow) throws DalException {
        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
        logger.info("[setAllowAutoMigration][{}] allow: {}", sourceIp, allow);
        configService.setAllowAutoMigration(allow);
    }

    @RequestMapping(value = "/config/sentinel/check/" + CLUSTER_NAME_PATH_VARIABLE + "/start", method = RequestMethod.POST)
    public RetMessage startSentinelCheck(HttpServletRequest request, @PathVariable String clusterName) throws DalException {
        checkClusterName(clusterName);
        ConfigModel config = configModel(request, null);
        config.setSubKey(clusterName);
        configService.startSentinelCheck(config);
        return RetMessage.createSuccessMessage("success");
    }

    @RequestMapping(value = {"/config/sentinel/check/" + CLUSTER_NAME_PATH_VARIABLE + "/stop/{maintainMinutes}",
            "/config/sentinel/check/" + CLUSTER_NAME_PATH_VARIABLE + "/stop"}, method = RequestMethod.POST)
    public RetMessage stopSentinelCheck(HttpServletRequest request, @PathVariable String clusterName,
                                        @PathVariable(required = false) Integer maintainMinutes) throws DalException {
        if (null == maintainMinutes || maintainMinutes <= 0) maintainMinutes = consoleConfig.getHealthCheckSuspendMinutes();
        maintainMinutes = Math.min(maintainMinutes, consoleConfig.getConfigDefaultRestoreHours() * 60);

        checkClusterName(clusterName);
        ConfigModel config = configModel(request, null);
        config.setSubKey(clusterName);
        configService.stopSentinelCheck(config, maintainMinutes);
        return RetMessage.createSuccessMessage("success");
    }

    @RequestMapping(value = "/config/sentinel/check/" + CLUSTER_NAME_PATH_VARIABLE, method = RequestMethod.GET)
    public ClusterConfigModel getSentinelCheckConfig(@PathVariable String clusterName) {
        if (StringUtil.isEmpty(clusterName)) throw new IllegalArgumentException("cluster can not be empty");
        return new ClusterConfigModel(clusterName, configService.shouldSentinelCheck(clusterName));
    }

    @RequestMapping(value = "/config/sentinel/check/exclude/all", method = RequestMethod.GET)
    public List<String> getAllSentinelCheckExcludeConfig() {
        List<ConfigModel> configModels = configService.getActiveSentinelCheckExcludeConfig();
        List<String> whitelist = configModels.stream().map(ConfigModel::getSubKey).collect(Collectors.toList());
        logger.info("[sentinel][whitelist] {}", whitelist);
        return whitelist;
    }

    @PostMapping(value = "/config/alert/" + CLUSTER_NAME_PATH_VARIABLE + "/start")
    public RetMessage startClusterAlert(HttpServletRequest request, @PathVariable String clusterName) throws DalException {
        checkClusterName(clusterName);
        ConfigModel config = configModel(request, null);
        config.setSubKey(clusterName);
        configService.startClusterAlert(config);
        return RetMessage.createSuccessMessage("success");
    }

    @PostMapping(value = {"/config/alert/" + CLUSTER_NAME_PATH_VARIABLE + "/stop/{maintainMinutes}",
            "/config/alert/" + CLUSTER_NAME_PATH_VARIABLE + "/stop"})
    public RetMessage stopClusterAlert(HttpServletRequest request, @PathVariable String clusterName,
                                        @PathVariable(required = false) Integer maintainMinutes) throws DalException {
        if (null == maintainMinutes || maintainMinutes <= 0) maintainMinutes = consoleConfig.getHealthCheckSuspendMinutes();
        maintainMinutes = Math.min(maintainMinutes, consoleConfig.getConfigDefaultRestoreHours() * 60);

        checkClusterName(clusterName);
        ConfigModel config = configModel(request, null);
        config.setSubKey(clusterName);
        configService.stopClusterAlert(config, maintainMinutes);
        return RetMessage.createSuccessMessage("success");
    }

    @GetMapping(value = "/config/alert/cluster/exclude/all")
    public List<String> getAllClusterAlertExcludeConfig() {
        List<ConfigModel> configModels = configService.getActiveClusterAlertExcludeConfig();
        List<String> whitelist = configModels.stream().map(ConfigModel::getSubKey).collect(Collectors.toList());
        logger.info("[alert][whitelist] {}", whitelist);
        return whitelist;
    }

    @PostMapping(value = {"/config/beacon/check/stop/{maintainMinutes}", "/config/beacon/check/stop"})
    public RetMessage stopBeaconCheck(HttpServletRequest request,
                                      @PathVariable(required = false) Integer maintainMinutes,
                                      @RequestBody BeaconCheckConfigRequest beaconCheckConfigRequest) throws Exception {
        if (null == maintainMinutes || maintainMinutes <= 0) {
            maintainMinutes = consoleConfig.getHealthCheckSuspendMinutes();
        }
        maintainMinutes = Math.min(maintainMinutes, consoleConfig.getConfigDefaultRestoreHours() * 60);
        validateSentinelBeaconCheckRequest(beaconCheckConfigRequest);
        beaconCheckConfigService.stopBeaconCheck(beaconCheckConfigRequest.getClusterName(),
                beaconCheckConfigRequest.getDc(), beaconCheckConfigRequest.getShards(), maintainMinutes);
        return RetMessage.createSuccessMessage("success");
    }

    @PostMapping(value = "/config/beacon/check/start")
    public RetMessage startBeaconCheck(HttpServletRequest request,
                                       @RequestBody BeaconCheckConfigRequest beaconCheckConfigRequest) throws Exception {
        validateSentinelBeaconCheckRequest(beaconCheckConfigRequest);
        beaconCheckConfigService.startBeaconCheck(beaconCheckConfigRequest.getClusterName(),
                beaconCheckConfigRequest.getDc(), beaconCheckConfigRequest.getShards());
        return RetMessage.createSuccessMessage("success");
    }

    private void validateSentinelBeaconCheckRequest(BeaconCheckConfigRequest beaconCheckConfigRequest) {
        if (beaconCheckConfigRequest == null) {
            throw new IllegalArgumentException("request body can not be empty");
        }
        beaconCheckConfigRequest.validate();
        String clusterName = beaconCheckConfigRequest.getClusterName();
        ClusterTbl cluster = clusterService.find(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(String.format("cluster %s not found", clusterName));
        }
        if (!consoleConfig.supportSentinelBeacon(cluster.getClusterOrgId(), clusterName)) {
            throw new IllegalArgumentException(String.format("cluster %s is not managed by beacon sentinel mode", clusterName));
        }
        beaconMetaService.validateSentinelBeaconOperatingDc(clusterName, beaconCheckConfigRequest.getDc());
    }

    private void checkClusterName(String clusterName) {
        if (StringUtil.isEmpty(clusterName)) throw new IllegalArgumentException("cluster can not be empty");
    }

    private ConfigModel configModel(HttpServletRequest request, ConfigModel configModel) {

        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
        ConfigModel config = new ConfigModel().setUpdateIP(sourceIp)
                .setUpdateUser(request.getRemoteUser());

        if(configModel != null && configModel.getUpdateUser() != null)
            config.setUpdateUser(configModel.getUpdateUser());

        return config;
    }
}
