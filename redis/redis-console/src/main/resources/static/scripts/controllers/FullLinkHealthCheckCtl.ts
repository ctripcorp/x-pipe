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
    $scope.resetElection = resetElection;
    $scope.disableResetElection = false;
    $scope.getDisableResetElection = getDisableResetElection;
    $scope.resetElectionErr = null;
    $scope.getResetElectionErr = getResetElectionErr;

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

    let timer = null;
    function resetElection(ip, port) {
        $scope.disableResetElection = true;
        KeeperContainerService.resetElection(ip, port, $stateParams.shardId).then(function (response) {
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

    function getResetElectionErr() {
        return $scope.resetElectionErr;
    }

    function doShowActions() {
        $scope.showActions = !$scope.showActions;
    }

    function getDisableResetElection() {
        return $scope.disableResetElection;
    }

}