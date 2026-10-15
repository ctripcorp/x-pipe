angular
    .module('index')
    .controller('FullLinkHealthCheckCtl', FullLinkHealthCheckCtl)
    .config(function($mdThemingProvider) {
        $mdThemingProvider.theme('green').backgroundPalette('light-green');
        $mdThemingProvider.theme('red').backgroundPalette('red');
        $mdThemingProvider.theme('orange').backgroundPalette('orange');
    });

FullLinkHealthCheckCtl.$inject = ['$rootScope', '$scope', '$window', '$stateParams', 'HealthCheckService', 'KeeperContainerService',
    'toastr', 'NgTableParams', 'AppUtil', '$interval'];

function FullLinkHealthCheckCtl($rootScope, $scope, $window, $stateParams, HealthCheckService, KeeperContainerService,
                                   toastr, NgTableParams, $interval) {
    $scope.masterRoles = [];
    $scope.slaveRoles = [];
    $scope.unknownRoles = [];
    $scope.shardCheckerHealthCheckResult = [];
    $scope.showActions=false;
    $scope.shardAllMeta = null;
    $scope.shardKeeperState = [];

    $scope.redisRoleHealthCheck = redisRoleHealthCheck;
    $scope.shardCheckerGroupHealthCheck = shardCheckerGroupHealthCheck;
    $scope.doShowActions = doShowActions;
    $scope.getShardAllMeta = getShardAllMeta;
    $scope.getShardKeeperState = getShardKeeperState;
    $scope.releaseRdb = releaseRdb;
    $scope.resetElection = resetElection;
    $scope.disableResetElection = false;
    $scope.disableReleaseRdb = false;
    $scope.getDisableResetElection = getDisableResetElection;
    $scope.getDisableReleaseRdb = getDisableReleaseRdb;
    $scope.resetElectionErr = null;
    $scope.releaseRdbErr = null;
    $scope.getResetElectionErr = getResetElectionErr;
    $scope.getReleaseRdbErr = getReleaseRdbErr;
    $scope.keeperMetaAddr = keeperMetaAddr;

    redisRoleHealthCheck();
    shardCheckerGroupHealthCheck();
    getShardAllMeta();
    getShardKeeperState();

    function redisRoleHealthCheck() {
        HealthCheckService.getShardRedisRole($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.masterRoles = [];
            $scope.slaveRoles = [];
            $scope.unknownRoles = [];
            response.forEach(function (item){
                if (item.role == null) {
                    $scope.unknownRoles.push(item)
                }else if (item.role.serverRole == "SLAVE") {
                    $scope.slaveRoles.push(item);
                } else if (item.role.serverRole == "MASTER") {
                    $scope.masterRoles.push(item);
                }
            })
        })
    }

    function shardCheckerGroupHealthCheck() {
        HealthCheckService.getShardCheckerHealthCheck($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.shardCheckerHealthCheckResult = [];
            response.forEach(function (item){
                $scope.shardCheckerHealthCheckResult.push(item);
            })
        })
    }

    function getShardAllMeta() {
        HealthCheckService.getShardAllMeta($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.shardAllMeta = null;
            $scope.shardAllMeta = response;
        })
    }

    function getShardKeeperState() {
        HealthCheckService.getShardKeeperState($stateParams.currentDcName, $stateParams.clusterName, $stateParams.shardName).then(function (response) {
            $scope.shardKeeperState = [];
            response.forEach(function (item){
                $scope.shardKeeperState.push(item);
            })
        })
    }

    let rdbTimer = null;
    // 传整个 keeper，避免再出现漏传 hostname 的情况；host 是后端反查后的真实 ip
    function releaseRdb(keeper) {
        $scope.disableReleaseRdb = true;
        KeeperContainerService.releaseRdb($stateParams.currentDcName, keeper.host, keeper.hostname, keeper.port, $stateParams.shardId).then(function (response) {
            if (response.state != 0) {
                $scope.disableReleaseRdb = false;
                $scope.releaseRdbErr = response.message;
                $('#releaseRdbErr').modal('show');
            } else {
                if (!rdbTimer) {
                    rdbTimer = setTimeout(() => {
                        clearTimeout(rdbTimer);
                        rdbTimer = null;
                        $scope.disableReleaseRdb = false;
                    }, 3000);
                }
            }
        }).catch(function (error){
            $scope.disableReleaseRdb = false;
            $scope.releaseRdbErr = "Status Code:" + error.status + " " + error.statusText + "\n" + error.data.exception;
            $('#releaseRdbErr').modal('show');
        })
    }

    let timer = null;
    function resetElection(keeper) {
        $scope.disableResetElection = true;
        KeeperContainerService.resetElection($stateParams.currentDcName, keeper.host, keeper.hostname, keeper.port, $stateParams.shardId).then(function (response) {
            if (response.state != 0) {
                $scope.disableResetElection = false;
                $scope.resetElectionErr = response.message;
                $('#resetElectionErr').modal('show');
            } else {
                if (!timer) {
                    timer = setTimeout(() => {
                        clearTimeout(timer);
                        timer = null;
                        $scope.disableResetElection = false;
                    }, 5000);
                }
            }
        }).catch(function (error){
            $scope.disableResetElection = false;
            $scope.resetElectionErr = "Status Code:" + error.status + " " + error.statusText + "\n" + error.data.exception;
            $('#resetElectionErr').modal('show');
        })
    }

    /**
     * meta 区块（SurviveKeepers / shardMeta.keepers）里的 KeeperMeta 展示成 hostname(ip):port。
     * 这里的 ip 是 meta 原始值，容器场景下是占位值，没有做反查，所以 hostname 放在前面；
     * 注意 KeeperMeta 的字段是 hostname，与 redis_tbl 的 hostName 大小写不同
     */
    function keeperMetaAddr(keeperMeta) {
        if (!keeperMeta) {
            return '';
        }
        var addr = keeperMeta.hostname
                ? (keeperMeta.ip ? keeperMeta.hostname + '(' + keeperMeta.ip + ')' : keeperMeta.hostname)
                : (keeperMeta.ip || '');
        return addr + ':' + keeperMeta.port;
    }

    function getResetElectionErr() {
        return $scope.resetElectionErr;
    }

    function getReleaseRdbErr() {
        return $scope.releaseRdbErr;
    }

    function doShowActions() {
        $scope.showActions = !$scope.showActions;
    }

    function getDisableResetElection() {
        return $scope.disableResetElection;
    }

    function getDisableReleaseRdb() {
        return $scope.disableReleaseRdb;
    }

}